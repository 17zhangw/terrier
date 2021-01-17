#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerAggKeylessExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                      execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;

  auto &num_cols = config_->sweep_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  for (auto col : num_cols) {
    for (auto row : row_nums) {
      bool is_skip_possible = rerun || settings_->skip_large_rows_runs_;
      if (row > settings_->warmup_rows_limit_ && is_skip_possible) {
        // Skip if large row classification reached
        continue;
      }

      int64_t car = 1;
      std::vector<int64_t> cars;
      while (car < row) {
        if (car > settings_->warmup_rows_limit_ && is_skip_possible) {
          // Skip if large row classification reached
          continue;
        }

        cars.emplace_back(car);
        car *= 2;
      }
      cars.emplace_back(row);

      for (auto car : cars) {
        std::vector<int64_t> args{col, 15, row, car};
        auto tbl = execution::sql::TableGenerator::GenerateTableName({type::TypeId::INTEGER}, {15}, row, car);
        mapping[tbl].emplace_back(std::move(args));
      }
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, {}, this, mode, std::move(map.second));
  }
}

void MiniRunnerAggKeylessExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                    execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto tbl_ints = iteration[1];
  auto row = iteration[2];
  auto car = iteration[3];

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto tuple_size = int_size * num_integers;
  auto num_col = num_integers;
  auto out_cols = num_col;
  auto out_size = tuple_size;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_BUILD, row,
                         0, num_col, 1, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_ITERATE, 1,
                         out_size, out_cols, 1, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, 1, out_size,
                         out_cols, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers, 0,
                                                       ", ", "", false, "");
    auto tbl_name =
        MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints, 0, row, car);
    query << "SELECT ";
    for (int i = 1; i <= num_integers; i++) {
      query << "SUM(" << (type::TypeUtil::TypeIdToString(type::TypeId::INTEGER)) << i << ")";
      if (i != num_integers) query << ", ";
    }

    query << " FROM " << tbl_name;
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
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    int iters = 1 + ((row <= settings_->warmup_rows_limit_ && car <= settings_->warmup_rows_limit_)
                         ? settings_->warmup_iterations_num_
                         : 0);
    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), iters, true, mode, exec_settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
