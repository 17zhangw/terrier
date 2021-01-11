#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "planner/plannodes/index_scan_plan_node.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerSortExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;

  auto is_topks = {0, 1};
  auto &num_cols = config_->sweep_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto is_topk : is_topks) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          if (row > settings_->warmup_rows_limit_ && (rerun || settings_->skip_large_rows_runs_)) {
            continue;
          }

          int64_t car = 1;
          std::vector<int64_t> cars;
          while (car < row) {
            cars.emplace_back(car);
            car *= 2;
          }
          cars.emplace_back(row);

          for (auto car : cars) {
            bool is_int = type == type::TypeId::INTEGER;
            std::vector<type::TypeId> type_vec{type::TypeId::INTEGER, type::TypeId::REAL};
            std::vector<uint32_t> col_vec{(is_int ? 15u : 0u), (is_int ? 0u : 15u)};
            auto tbl = execution::sql::TableGenerator::GenerateTableName(type_vec, col_vec, row, is_topk ? row : car);

            std::vector<int64_t> args{
                (is_int ? col : 0), (is_int ? 0 : col), (is_int ? 15 : 0), (is_int ? 0 : 15), row, car, is_topk};
            mapping[tbl].emplace_back(std::move(args));
          }
        }
      }
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, this, mode, std::move(map.second));
  }
}

void MiniRunnerSortExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                              execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_reals = iteration[1];
  auto tbl_ints = iteration[2];
  auto tbl_reals = iteration[3];
  auto row = iteration[4];
  auto car = iteration[5];
  auto is_topk = iteration[6];

  // OU features
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_integers + real_size * num_reals;
  auto num_col = num_integers + num_reals;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  auto table_car = car;
  auto output_num = row;
  selfdriving::ExecutionOperatingUnitType build_ou_type = selfdriving::ExecutionOperatingUnitType::SORT_BUILD;
  if (is_topk == 1) {
    build_ou_type = selfdriving::ExecutionOperatingUnitType::SORT_TOPK_BUILD;
    table_car = row;
    output_num = car;
  }
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, table_car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), build_ou_type, row, tuple_size, num_col, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SORT_ITERATE,
                         output_num, tuple_size, num_col, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, output_num,
                         tuple_size, num_col, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  // Construct query
  std::string query;
  {
    std::stringstream query_ss;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::REAL, num_integers,
                                                       num_reals, ", ", "", false, "");
    auto tbl_name = MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::REAL, tbl_ints,
                                                           tbl_reals, row, table_car);
    query_ss << "SELECT " << (cols) << " FROM " << tbl_name << " ORDER BY " << (cols);
    if (is_topk == 1) query_ss << " LIMIT " << car;
    query = query_ss.str();
  }

  {
    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query;
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    int iters = 1 + (row <= settings_->warmup_rows_limit_ ? settings_->warmup_iterations_num_ : 0);
    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), iters, true, mode, exec_settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
