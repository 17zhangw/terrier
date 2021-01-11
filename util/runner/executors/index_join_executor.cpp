#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/index_join_plan_node.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerIndexJoinExecutor::IndexNLJoinChecker(catalog::db_oid_t db_oid, std::string build_tbl, size_t num_cols,
                                                     common::ManagedPointer<transaction::TransactionContext> txn,
                                                     planner::AbstractPlanNode *plan) {
  if (plan->GetPlanNodeType() != planner::PlanNodeType::INDEXNLJOIN) throw "Expected IndexNLJoin";
  if (plan->GetChildrenSize() != 1) throw "Expected 1 child";
  if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected child IdxScan";

  auto *nlplan = reinterpret_cast<const planner::IndexJoinPlanNode *>(plan);
  if (nlplan->GetLoIndexColumns().size() != num_cols) throw "Number keys mismatch";

  auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto build_oid = accessor->GetTableOid(std::move(build_tbl));
  if (nlplan->GetTableOid() != build_oid) throw "inner index does not match";
}

void MiniRunnerIndexJoinExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                     execution::vm::ExecutionMode mode) {
  if (rerun) {
    return;
  }

  auto &key_sizes = config_->sweep_col_nums_;
  auto idx_sizes = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto key_size : key_sizes) {
    for (size_t j = 0; j < idx_sizes.size(); j++) {
      auto inner_tbl = execution::sql::TableGenerator::GenerateTableName(types, {15}, idx_sizes[j], idx_sizes[j]);
      if (idx_sizes[j] >= settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
        continue;
      }

      for (size_t i = 0; i < idx_sizes.size(); i++) {
        // TODO: pretty nasty inefficiency
        auto outer_tbl = execution::sql::TableGenerator::GenerateTableName(types, {15}, idx_sizes[i], idx_sizes[i]);

        // Create inner
        scheduler->CreateSchedule({outer_tbl, inner_tbl}, this, mode, {{key_size, 0, idx_sizes[j], 1}});
        scheduler->CreateSchedule({outer_tbl, inner_tbl}, this, mode, {{key_size, idx_sizes[i], idx_sizes[j], -1}});

        // Drop inner
        scheduler->CreateSchedule({outer_tbl, inner_tbl}, this, mode, {{key_size, 0, idx_sizes[j], 0}});
      }
    }
  }
}

void MiniRunnerIndexJoinExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                   execution::vm::ExecutionMode mode) {
  auto type = type::TypeId::INTEGER;
  auto tbl_cols = 15;
  auto key_num = iteration[0];
  auto outer = iteration[1];
  auto inner = iteration[2];
  auto is_build = iteration[3];

  if (outer == 0) {
    if (is_build < 0) {
      throw "Invalid is_build argument for IndexJoin";
    }

    MiniRunnersExecUtil::HandleBuildDropIndex(*db_main_, settings_->db_oid_, is_build != 0, tbl_cols, inner, key_num,
                                              type);
    return;
  }

  auto type_size = type::TypeUtil::GetTypeTrueSize(type);
  auto tuple_size = type_size * key_num;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;

  // We only ever emit min(outer, inner) # of tuples
  // Even though there are no matches, it still might be a good idea to see what the relation is
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT,
                         std::min(inner, outer), tuple_size, key_num, 0, 1, 0, 0);

  // Outer table scan happens
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, outer,
                         tuple_size, key_num, outer, 1, 0, 0);

  // For each in outer, match 1 tuple in inner
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, inner,
                         tuple_size, key_num, 1, 1, outer, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  auto inner_tbl = MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, inner, inner);
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, ", ", "a", false, "");
    auto predicate =
        MiniRunnersSqlUtil::ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, " AND ", "a", true, "b");
    auto outer_tbl = MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, outer, outer);
    query << "SELECT " << cols << " FROM " << outer_tbl << " AS a, " << inner_tbl << " AS b WHERE " << predicate;
    query_final = query.str();
  }

  {
    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query_final;
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    optimize.checker = std::bind(&MiniRunnerIndexJoinExecutor::IndexNLJoinChecker, this, settings_->db_oid_, inner_tbl,
                                 key_num, std::placeholders::_1, std::placeholders::_2);
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), 1, true, mode, exec_settings};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
