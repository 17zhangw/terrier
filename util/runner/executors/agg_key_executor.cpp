#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

std::map<std::string, MiniRunnerArguments> MiniRunnerAggKeyExecutor::ConstructTableArgumentsMapping(
    bool rerun, execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;

  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::VARCHAR};
  const std::vector<uint32_t> *num_cols;
  for (auto type : types) {
    if (type == type::TypeId::VARCHAR)
      num_cols = &config_->sweep_varchar_col_nums_;
    else
      num_cols = &config_->sweep_col_nums_;

    for (auto col : *num_cols) {
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
            continue;
          }

          cars.push_back(car);
          car *= 2;
        }
        cars.push_back(row);

        for (auto car : cars) {
          uint32_t int_cols = (type == type::TypeId::INTEGER) ? 15 : 0;
          uint32_t varchar_cols = (type == type::TypeId::VARCHAR) ? 5 : 0;
          auto tbl = execution::sql::TableGenerator::GenerateTableName(types, {int_cols, varchar_cols}, row, car);

          std::vector<int64_t> args{(type == type::TypeId::INTEGER) ? col : 0,
                                    (type == type::TypeId::INTEGER) ? 0 : col,
                                    static_cast<int64_t>(int_cols),
                                    static_cast<int64_t>(varchar_cols),
                                    row,
                                    car};
          mapping[tbl].emplace_back(MiniRunnerIterationArgument{std::move(args)});
        }
      }
    }
  }

  return mapping;
}

void MiniRunnerAggKeyExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                execution::vm::ExecutionMode mode) {
  auto num_integers = iteration.state[0];
  auto num_varchars = iteration.state[1];
  auto tbl_ints = iteration.state[2];
  auto tbl_varchars = iteration.state[3];
  auto row = iteration.state[4];
  auto car = iteration.state[5];

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_varchars;
  for (auto i = 0; i < num_varchars; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(type::TypeId::VARCHAR, &tuple_size, &num_col);
  }
  auto out_cols = num_col + 1;     // pulling the count(*) out
  auto out_size = tuple_size + 4;  // count(*) is an integer

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe1_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_BUILD, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::AGGREGATE_ITERATE, car,
                         out_size, out_cols, car, 1, 0, 0);
  pipe1_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, car, out_size,
                         out_cols, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(2), std::move(pipe0_vec));
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe1_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, type::TypeId::VARCHAR, num_integers,
                                                       num_varchars, ", ", "", false, "");
    auto tbl_name = MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, type::TypeId::VARCHAR, tbl_ints,
                                                           tbl_varchars, row, car);
    query << "SELECT COUNT(*), " << cols << " FROM " << tbl_name << " GROUP BY " << cols;
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

    MiniRunnersExecUtil::ExecuteRequest req{*db_main_,
                                            settings_->db_oid_,
                                            equery.first.get(),
                                            equery.second.get(),
                                            settings_->warmup_iterations_num_ + 1,
                                            true,
                                            mode,
                                            exec_settings,
                                            {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
