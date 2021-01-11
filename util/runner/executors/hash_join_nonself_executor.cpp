#include <type_traits>

#include "planner/plannodes/seq_scan_plan_node.h"
#include "optimizer/cost_model/forced_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

  void MiniRunnerHashJoinNonSelfExecutor::JoinNonSelfChecker(
      catalog::db_oid_t db_oid,
      std::string build_tbl, common::ManagedPointer<transaction::TransactionContext> txn,
      planner::AbstractPlanNode *plan) {
    auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
    auto accessor = catalog->GetAccessor(txn, db_oid, DISABLED);
    auto build_oid = accessor->GetTableOid(std::move(build_tbl));

    if (plan->GetPlanNodeType() != planner::PlanNodeType::HASHJOIN) throw "Expected HashJoin";
    if (plan->GetChild(0)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Left SeqScan";
    if (plan->GetChild(1)->GetPlanNodeType() != planner::PlanNodeType::SEQSCAN) throw "Expected Right SeqScan";

    // Assumes the build side is the left (since that is the HashJoinLeftTranslator)
    auto *l_scan = reinterpret_cast<const planner::SeqScanPlanNode *>(plan->GetChild(0));
    if (l_scan->GetTableOid() != build_oid) throw "Join order incorrect";
  }

void MiniRunnerHashJoinNonSelfExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun, execution::vm::ExecutionMode mode) {
  if (rerun) {
    // Don't execute on rerun
    return;
  }

  auto &num_cols = config_->sweep_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (size_t i = 0; i < row_nums.size(); i++) {
        auto build_rows = row_nums[i];
        auto build_car = row_nums[i];
        uint32_t int_cols = (type == type::TypeId::INTEGER) ? 15 : 0;
        uint32_t bigint_cols = (type == type::TypeId::BIGINT) ? 15 : 0;
        auto build_tbl = execution::sql::TableGenerator::GenerateTableName(types, {int_cols}, build_rows, build_car);

        for (size_t j = i + 1; j < row_nums.size(); j++) {
          auto probe_rows = row_nums[j];
          auto probe_car = row_nums[j];
          auto probe_tbl = execution::sql::TableGenerator::GenerateTableName(types, {int_cols}, probe_rows, probe_car);
          if (probe_rows > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
            continue;
          }

          auto matched_car = row_nums[i];
          std::vector<int64_t> args{
            (type == type::TypeId::INTEGER) ? col : 0,
            (type == type::TypeId::BIGINT) ? col : 0,
            int_cols,
            bigint_cols,
            build_rows,
            build_car,
            probe_rows,
            probe_car,
            matched_car} ;

          scheduler->CreateSchedule({build_tbl, probe_tbl}, this, mode, {std::move(args)});
        }
      }
    }
  }
}

void MiniRunnerHashJoinNonSelfExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                 execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_bigints = iteration[1];
  auto tbl_ints = iteration[2];
  auto tbl_bigints = iteration[3];
  auto build_row = iteration[4];
  auto build_car = iteration[5];
  auto probe_row = iteration[6];
  auto probe_car = iteration[7];
  auto matched_car = iteration[8];

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, build_row,
                         tuple_size, num_col, build_car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD,
                         build_row, tuple_size, num_col, build_car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, probe_row,
                         tuple_size, num_col, probe_car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE,
                         probe_row, tuple_size, num_col, matched_car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, matched_car,
                         tuple_size, num_col, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  auto build_tbl = MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, build_row, build_car);
  auto probe_tbl = MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, tbl_bigints, probe_row, probe_car);

  std::string query_final;
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, ", ", "b", false, "");
    auto predicate = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, num_bigints, " AND ", build_tbl, true, "b");
    query << "SELECT " << cols << " FROM " << build_tbl << ", " << probe_tbl << " as b WHERE " << predicate;
    query_final = query.str();
  }

  {
    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query_final;
    optimize.cost_model = std::make_unique<optimizer::ForcedCostModel>(true);
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    optimize.checker = std::bind(&MiniRunnerHashJoinNonSelfExecutor::JoinNonSelfChecker, this, settings_->db_oid_, build_tbl, std::placeholders::_1, std::placeholders::_2);
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{*db_main_,
                                            settings_->db_oid_,
                                            equery.first.get(),
                                            equery.second.get(),
                                            1,
                                            true,
                                            mode,
                                            exec_settings,
                                            {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
