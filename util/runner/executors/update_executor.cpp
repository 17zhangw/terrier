#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/update_plan_node.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void UpdateIndexScanChecker(common::ManagedPointer<transaction::TransactionContext> txn,
                            planner::AbstractPlanNode *plan) {
  if (plan->GetPlanNodeType() != planner::PlanNodeType::UPDATE) throw "Expected Update";
  auto *upd = reinterpret_cast<planner::UpdatePlanNode *>(plan);
  if (!upd->GetIndexOids().empty()) throw "Update index oids not empty";
  if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::INDEXSCAN) throw "Expected IndexScan";
}

std::map<std::string, MiniRunnerArguments> MiniRunnerUpdateExecutor::ConstructTableArgumentsMapping(
    bool rerun, execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;
  auto &idx_key = config_->sweep_update_index_col_nums_;
  auto &update_keys = config_->sweep_update_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto type : types) {
    for (auto idx_key_size : idx_key) {
      for (auto update_key : update_keys) {
        if (idx_key_size + update_key >= 15) continue;

        for (auto row_num : row_nums) {
          if (row_num > settings_->updel_limit_) continue;

          auto tbl = execution::sql::TableGenerator::GenerateTableName({type}, {15}, row_num, row_num);
          std::vector<int64_t> template_args{(type == type::TypeId::INTEGER) ? idx_key_size : 0,
                                             (type == type::TypeId::BIGINT) ? idx_key_size : 0,
                                             update_key,
                                             (type == type::TypeId::INTEGER) ? 15 : 0,
                                             (type == type::TypeId::BIGINT) ? 15 : 0,
                                             row_num};

          int64_t lookup_size = 1;
          std::vector<int64_t> lookups;
          while (lookup_size <= row_num) {
            if (lookup_size > settings_->warmup_rows_limit_ && (rerun || settings_->skip_large_rows_runs_)) {
              continue;
            }

            lookups.push_back(lookup_size);
            lookup_size *= 2;
          }

          if (!lookups.empty()) {
            // Special argument used to indicate a build index
            // We need to do this to prevent update/delete from unintentionally
            // updating multiple indexes. This way, there will only be 1 index
            // on the table at a given time.
            std::vector<int64_t> arg_vec{template_args};
            arg_vec.emplace_back(0);
            arg_vec.emplace_back(1);
            mapping[tbl].emplace_back(MiniRunnerIterationArgument{std::move(arg_vec)});
          }

          for (auto lookup : lookups) {
            std::vector<int64_t> arg_vec{template_args};
            arg_vec.emplace_back(lookup);
            arg_vec.emplace_back(-1);
            mapping[tbl].emplace_back(MiniRunnerIterationArgument{std::move(arg_vec)});
          }

          if (!lookups.empty()) {
            // Special argument used to indicate a drop index
            std::vector<int64_t> arg_vec{template_args};
            arg_vec.emplace_back(0);
            arg_vec.emplace_back(0);
            mapping[tbl].emplace_back(MiniRunnerIterationArgument{std::move(arg_vec)});
          }
        }
      }
    }
  }

  return mapping;
}

void MiniRunnerUpdateExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                execution::vm::ExecutionMode mode) {
  auto num_integers = iteration.state[0];
  auto num_bigints = iteration.state[1];
  auto update_keys = iteration.state[2];
  auto tbl_ints = iteration.state[3];
  auto tbl_bigints = iteration.state[4];
  auto row = iteration.state[5];
  auto car = iteration.state[6];
  auto is_build = iteration.state[7];

  bool is_first_type = tbl_ints != 0;
  auto type = is_first_type ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = is_first_type ? (int_size * update_keys) : (bigint_size * update_keys);
  auto num_col = is_first_type ? num_integers : num_bigints;
  auto idx_size = is_first_type ? (int_size * num_col) : (bigint_size * num_col);

  if (car == 0) {
    // A lookup size of 0 indicates a special query
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteUpdate";
    }

    MiniRunnersExecUtil::HandleBuildDropIndex(*db_main_, settings_->db_oid_, is_build != 0, tbl_ints + tbl_bigints, row,
                                              num_col, type);
    return;
  }

  // UPDATE [] SET [non-indexed columns] = [non-indexed clumns] WHERE [indexed cols]
  //
  // This will generate an UPDATE with an index scan child. Furthermore, the code-gen
  // code will not do a DELETE followed by an INSERT on the underlying table since
  // the UPDATE statement does not update any indexed columns.
  std::string query_final;
  {
    std::stringstream query;
    std::string tbl =
        MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_ints + tbl_bigints, 0, row, row);
    query << "UPDATE " << tbl << " SET ";

    std::vector<catalog::Schema::Column> cols;
    {
      uint64_t limit = is_first_type ? tbl_ints : tbl_bigints;
      limit = std::min(limit, static_cast<uint64_t>(num_col + update_keys));
      auto type_name = type::TypeUtil::TypeIdToString(type);
      for (uint64_t j = num_col + 1; j <= limit; j++) {
        query << type_name << j << " = " << type_name << j;
        if (j != limit) query << ", ";
      }
    }

    auto predicate = MiniRunnersSqlUtil::ConstructIndexScanPredicate(type, num_col, car);
    query << " WHERE " << predicate;

    query_final = query.str();
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::UPDATE, car,
                         tuple_size, update_keys, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, row,
                         idx_size, num_col, car, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  int num_iters = settings_->warmup_rows_limit_ + 1;
  std::vector<std::vector<parser::ConstantValueExpression>> real_params;
  MiniRunnersSqlUtil::GenIdxScanParameters(type, row, car, num_iters, &real_params);

  {
    std::vector<type::TypeId> param_types;
    param_types.push_back(type);
    if (car > 1) {
      param_types.push_back(type);
    }

    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query_final;
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    optimize.checker = std::bind(UpdateIndexScanChecker, std::placeholders::_1, std::placeholders::_2);
    optimize.param_types = common::ManagedPointer(&param_types);
    optimize.params = common::ManagedPointer(&real_params[0]);
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(),    equery.second.get(), num_iters, false,
        mode,      exec_settings,      std::move(real_params)};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
