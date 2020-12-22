#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

std::map<std::string, MiniRunnerArguments> MiniRunnerSeqScanExecutor::ConstructTableArgumentsMapping(
    bool rerun, execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;

  // Non mixed arguments
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL, type::TypeId::VARCHAR};
  const std::vector<uint32_t> *num_cols;
  for (auto type : types) {
    if (type == type::TypeId::VARCHAR)
      num_cols = &config_->sweep_varchar_col_nums_;
    else
      num_cols = &config_->sweep_col_nums_;

    for (auto col : *num_cols) {
      for (auto row : row_nums) {
        if (row > settings_->warmup_rows_limit_ && (rerun || settings_->skip_large_rows_runs_)) {
          // Skip if large row classification reached
          continue;
        }

        int64_t car = 1;
        std::vector<int64_t> cars;
        while (car < row) {
          cars.push_back(car);
          car *= 2;
        }
        cars.push_back(row);

        for (auto car : cars) {
          uint32_t int_cols = (type == type::TypeId::INTEGER) ? 15 : 0;
          uint32_t real_cols = (type == type::TypeId::REAL) ? 15 : 0;
          uint32_t varchar_cols = (type == type::TypeId::VARCHAR) ? 5 : 0;
          auto tbl =
              execution::sql::TableGenerator::GenerateTableName(types, {int_cols, real_cols, varchar_cols}, row, car);

          std::vector<int64_t> args{(type == type::TypeId::INTEGER) ? col : 0,
                                    (type == type::TypeId::INTEGER) ? 0 : col,
                                    static_cast<int64_t>(int_cols),
                                    static_cast<int64_t>(real_cols + varchar_cols),
                                    row,
                                    car,
                                    (varchar_cols != 0)};
          mapping[tbl].emplace_back(MiniRunnerIterationArgument{std::move(args)});
        }
      }
    }
  }

  // Mixed scan requests
  std::vector<std::vector<int64_t>> args;
  MiniRunnersSqlUtil::GenMixedArguments(&args, *settings_, *config_, row_nums, 0);
  MiniRunnersSqlUtil::GenMixedArguments(&args, *settings_, *config_, row_nums, 1);
  for (const auto &arg : args) {
    std::vector<type::TypeId> types;
    if (arg[arg.size() - 1] == 0) {
      // Last index indicates varchar
      types = std::vector<type::TypeId>{type::TypeId::INTEGER, type::TypeId::REAL};
    } else {
      types = std::vector<type::TypeId>{type::TypeId::INTEGER, type::TypeId::VARCHAR};
    }

    std::vector<uint32_t> col_dist{static_cast<uint32_t>(arg[2]), static_cast<uint32_t>(arg[3])};
    auto tbl = execution::sql::TableGenerator::GenerateTableName(types, col_dist, arg[4], arg[5]);
    mapping[tbl].emplace_back(MiniRunnerIterationArgument{arg});
  }

  return mapping;
}

void MiniRunnerSeqScanExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                 execution::vm::ExecutionMode mode) {
  auto num_integers = iteration.state[0];
  auto num_mix = iteration.state[1];
  auto tbl_ints = iteration.state[2];
  auto tbl_mix = iteration.state[3];
  auto row = iteration.state[4];
  auto car = iteration.state[5];
  auto varchar_mix = iteration.state[6];

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  type::TypeId mix_type;
  if (varchar_mix == 1)
    mix_type = type::TypeId::VARCHAR;
  else
    mix_type = type::TypeId::REAL;
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_mix;

  // Adjust for type of MIX
  for (auto i = 0; i < num_mix; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(mix_type, &tuple_size, &num_col);
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::SEQ_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, row,
                         tuple_size, num_col, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, mix_type, num_integers, num_mix, ", ", "",
                                                       false, "");
    auto tbl_name =
        MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, mix_type, tbl_ints, tbl_mix, row, car);
    query << "SELECT " << (cols) << " FROM " << tbl_name;
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

    int64_t num_iters = 1 + (row <= settings_->warmup_rows_limit_ ? settings_->warmup_iterations_num_ : 0);
    MiniRunnersExecUtil::ExecuteRequest req{*db_main_,
                                            settings_->db_oid_,
                                            equery.first.get(),
                                            equery.second.get(),
                                            num_iters,
                                            true,
                                            mode,
                                            exec_settings,
                                            {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
