#include <type_traits>

#include "optimizer/cost_model/forced_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerHashJoinSelfExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                        execution::vm::ExecutionMode mode) {
  if (rerun) {
    // Don't execute on rerun
    return;
  }

  std::map<std::string, MiniRunnerArguments> mapping;

  auto &num_cols = config_->sweep_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER};
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : row_nums) {
        if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
          continue;
        }

        int64_t car = 1;
        std::vector<int64_t> cars;
        while (car < row) {
          if (row * row / car <= settings_->data_rows_limit_) {
            cars.push_back(car);
          }

          car *= 2;
        }
        cars.push_back(row);

        for (auto car : cars) {
          uint32_t int_cols = (type == type::TypeId::INTEGER) ? 15 : 0;
          uint32_t bigint_cols = (type == type::TypeId::BIGINT) ? 15 : 0;
          auto tbl = execution::sql::TableGenerator::GenerateTableName(types, {int_cols}, row, car);

          std::vector<int64_t> args{(type == type::TypeId::INTEGER) ? col : 0,
                                    (type == type::TypeId::BIGINT) ? col : 0,
                                    static_cast<int64_t>(int_cols),
                                    static_cast<int64_t>(bigint_cols),
                                    row,
                                    car};
          mapping[tbl].emplace_back(std::move(args));
        }
      }
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, {}, this, mode, std::move(map.second));
  }
}

void MiniRunnerHashJoinSelfExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                      execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_bigints = iteration[1];
  auto tbl_ints = iteration[2];
  auto tbl_bigints = iteration[3];
  auto row = iteration[4];
  auto car = iteration[5];

  // Size of the scan tuple
  // Size of hash key size, probe key size
  // Size of output since only output 1 side
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;

  auto hj_output = row * row / car;
  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_BUILD, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::HASHJOIN_PROBE, row,
                         tuple_size, num_col, hj_output, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, hj_output,
                         tuple_size, num_col, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto tbl_name = MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::BIGINT, tbl_ints,
                                                           tbl_bigints, row, car);
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers,
                                                       num_bigints, ", ", "b", false, "");
    auto predicate = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::BIGINT, num_integers,
                                                            num_bigints, " AND ", tbl_name, true, "b");
    query << "SELECT " << cols << " FROM " << tbl_name << ", " << tbl_name << " as b WHERE " << predicate;
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
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), 1, true, mode, exec_settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
