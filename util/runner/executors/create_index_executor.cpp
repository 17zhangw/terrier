#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerCreateIndexExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                       execution::vm::ExecutionMode mode) {
  if (rerun) {
    return;
  }

  std::map<std::string, MiniRunnerArguments> mapping;
  auto &num_threads = config_->sweep_index_create_threads_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);

  // Create a nice column ordering
  std::vector<uint32_t> num_cols;
  {
    std::unordered_set<uint32_t> col_set;
    col_set.insert(config_->sweep_col_nums_.begin(), config_->sweep_col_nums_.end());
    col_set.insert(config_->sweep_index_col_nums_.begin(), config_->sweep_index_col_nums_.end());

    num_cols.reserve(col_set.size());
    for (auto &col : col_set) {
      num_cols.push_back(col);
    }
  }
  std::sort(num_cols.begin(), num_cols.end(), std::less<>());

  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto thread : num_threads) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
            // Skip condition
            continue;
          }

          int64_t car = 1;
          // Only use one cardinality for now since we can't track any cardinality anyway
          // TODO(lin): Augment the create index runner with different cardinalities when we're able to
          //  estimate/track the cardinality.
          if (row > settings_->create_index_small_limit_) {
            // For these, we get a memory explosion if the cardinality is too low.
            while (car < row) {
              car *= 2;
            }
            car = car / (pow(2, settings_->create_index_large_cardinality_num_));
          }

          auto tbl = execution::sql::TableGenerator::GenerateTableName({type}, {15}, row, car);
          std::vector<int64_t> args{(type == type::TypeId::INTEGER) ? col : 0,
                                    (type == type::TypeId::BIGINT) ? col : 0,
                                    (type == type::TypeId::INTEGER) ? 15 : 0,
                                    (type == type::TypeId::BIGINT) ? 15 : 0,
                                    row,
                                    car,
                                    0,
                                    thread};
          mapping[tbl].emplace_back(std::move(args));
        }
      }
    }
  }

  // Generates INTEGER + VARCHAR
  std::vector<std::vector<int64_t>> args;
  MiniRunnersSqlUtil::GenMixedArguments(&args, *settings_, *config_, row_nums, 1);
  for (auto thread : num_threads) {
    for (auto arg : args) {
      auto row = arg[4];
      auto arg_car = arg[5];
      if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
        continue;
      }

      if (row > settings_->create_index_small_limit_) {
        // For these, we get a memory explosion if the cardinality is too low.
        int64_t car = 1;
        while (car < row) {
          car *= 2;
        }

        // This car is the ceiling
        car = car / (pow(2, settings_->create_index_large_cardinality_num_));
        // Only use one cardinality for now since we can't track any cardinality anyway
        if (arg_car != car) {
          continue;
        }
      }

      std::vector<type::TypeId> types;
      if (arg[arg.size() - 1] == 0) {
        // Last index indicates varchar
        types = std::vector<type::TypeId>{type::TypeId::INTEGER, type::TypeId::REAL};
      } else {
        types = std::vector<type::TypeId>{type::TypeId::INTEGER, type::TypeId::VARCHAR};
      }

      std::vector<uint32_t> col_dist{static_cast<uint32_t>(arg[2]), static_cast<uint32_t>(arg[3])};
      auto tbl = execution::sql::TableGenerator::GenerateTableName(types, col_dist, row, arg_car);
      arg.push_back(thread);
      mapping[tbl].emplace_back(std::move(arg));
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, this, mode, std::move(map.second));
  }
}

void MiniRunnerCreateIndexExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                     execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_mix = iteration[1];
  auto tbl_ints = iteration[2];
  auto tbl_mix = iteration[3];
  auto row = iteration[4];
  auto car = iteration[5];
  auto varchar_mix = iteration[6];
  auto num_threads = iteration[7];

  // Only generate counters if executing in parallel
  auto exec_settings = MiniRunnersExecUtil::GetParallelExecutionSettings(num_threads, num_threads != 0);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  type::TypeId mix_type;
  if (varchar_mix == 1)
    mix_type = type::TypeId::VARCHAR;
  else
    mix_type = type::TypeId::BIGINT;
  size_t tuple_size = int_size * num_integers;
  size_t num_col = num_integers + num_mix;

  // Adjust for type of MIX
  for (auto i = 0; i < num_mix; i++) {
    selfdriving::OperatingUnitRecorder::AdjustKeyWithType(mix_type, &tuple_size, &num_col);
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::CREATE_INDEX, row,
                         tuple_size, num_col, car, 1, 0, num_threads);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  std::string idx_name("runner_idx");
  {
    std::stringstream query;
    auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId::INTEGER, mix_type, num_integers, num_mix, ", ", "",
                                                       false, "");
    auto tbl_name =
        MiniRunnersSqlUtil::ConstructTableName(type::TypeId::INTEGER, mix_type, tbl_ints, tbl_mix, row, car);
    query << "CREATE INDEX " << idx_name << " ON " << tbl_name << " (" << cols << ")";
    query_final = query.str();
  }

  {
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query_final;
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), 1, true, mode, exec_settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
    MiniRunnersExecUtil::DropIndexByName(*db_main_, settings_->db_oid_, idx_name);
  }
}

};  // namespace noisepage::runner
