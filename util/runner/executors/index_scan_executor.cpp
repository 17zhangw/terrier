#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void IndexScanChecker(size_t num_keys, common::ManagedPointer<transaction::TransactionContext> txn,
                      planner::AbstractPlanNode *plan) {
  if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";

  auto *idx_scan = reinterpret_cast<planner::IndexScanPlanNode *>(plan);
  if (idx_scan->GetLoIndexColumns().size() != num_keys) throw "Number keys mismatch";
}

std::map<std::string, MiniRunnerArguments> MiniRunnerIndexScanExecutor::ConstructTableArgumentsMapping(
    bool rerun, execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;

  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT, type::TypeId::VARCHAR};
  auto idx_sizes = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto &lookup_sizes = config_->sweep_index_lookup_sizes_;
  const std::vector<uint32_t> *key_sizes;
  for (auto type : types) {
    uint32_t tbl_cols = (type == type::TypeId::VARCHAR) ? 5 : 15;
    if (type == type::TypeId::VARCHAR)
      key_sizes = &config_->sweep_varchar_index_col_nums_;
    else
      key_sizes = &config_->sweep_index_col_nums_;

    for (auto key_size : *key_sizes) {
      for (auto idx_size : idx_sizes) {
        bool real_iteration = false;
        std::vector<std::vector<int64_t>> args_vector;
        auto tbl = execution::sql::TableGenerator::GenerateTableName({type}, {tbl_cols}, idx_size, idx_size);

        // Construct index
        args_vector.push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, 0, 1});

        // Lookup indexes
        for (auto lookup_size : lookup_sizes) {
          if (lookup_size <= idx_size) {
            if (lookup_size > settings_->warmup_rows_limit_ && (rerun || settings_->skip_large_rows_runs_)) {
              continue;
            }

            args_vector.push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, lookup_size, -1});
            real_iteration = true;
          }
        }

        // Drop index
        args_vector.push_back({static_cast<int64_t>(type), tbl_cols, key_size, idx_size, 0, 0});

        if (real_iteration) {
          for (auto &arg : args_vector) {
            mapping[tbl].emplace_back(MiniRunnerIterationArgument{arg});
          }
        }
      }
    }
  }

  return mapping;
}

void MiniRunnerIndexScanExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                   execution::vm::ExecutionMode mode) {

  auto type = static_cast<type::TypeId>(iteration.state[0]);
  auto tbl_cols = iteration.state[1];
  size_t key_num = iteration.state[2];
  auto num_rows = iteration.state[3];
  auto lookup_size = iteration.state[4];
  auto is_build = iteration.state[5];

  if (lookup_size == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexScan";
    }

    MiniRunnersExecUtil::HandleBuildDropIndex((*db_main_), settings_->db_oid_, is_build != 0, tbl_cols, num_rows, key_num, type);
    return;
  }

  // Build operating unit features
  size_t tuple_size = 0;
  size_t num_col = key_num;
  for (size_t i = 0; i < num_col; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(type, &tuple_size, &key_num);
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, lookup_size,
                         tuple_size, key_num, 0, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, num_rows,
                         tuple_size, key_num, lookup_size, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  // Generate query
  std::string query;
  {
    std::stringstream query_ss;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type, type::TypeId::INVALID, num_col, 0, ", ", "", false, "");
    std::string predicate = MiniRunnersSqlUtil::ConstructIndexScanPredicate(type, num_col, lookup_size);
    auto table_name = MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, num_rows, num_rows);
    query_ss << "SELECT " << cols << " FROM  " << table_name << " WHERE " << predicate;
    query = query_ss.str();
  }

  // Generate parameters
  int64_t num_iters = 1 + settings_->warmup_iterations_num_;
  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  MiniRunnersSqlUtil::GenIdxScanParameters(type, num_rows, lookup_size, num_iters, &real_params);

  {
    std::vector<type::TypeId> param_types;
    param_types.push_back(type);
    param_types.push_back(type);

    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query;
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    optimize.checker = std::bind(IndexScanChecker, num_col, std::placeholders::_1, std::placeholders::_2);
    optimize.param_types = common::ManagedPointer(&param_types);
    optimize.params = common::ManagedPointer(&real_params[0]);
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{*db_main_,
                                            settings_->db_oid_,
                                            equery.first.get(),
                                            equery.second.get(),
                                            num_iters,
                                            true,
                                            mode,
                                            exec_settings,
                                            std::move(real_params)};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
