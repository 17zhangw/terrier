#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"

namespace noisepage::runner {

void MiniRunnerDeleteExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                  execution::vm::ExecutionMode mode) {
  std::map<std::string, MiniRunnerArguments> mapping;
  auto &idx_key = config_->sweep_update_index_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  std::vector<type::TypeId> types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  for (auto type : types) {
    for (auto idx_key_size : idx_key) {
      for (auto row_num : row_nums) {
        if (row_num > settings_->updel_limit_) continue;

        auto tbl = execution::sql::TableGenerator::GenerateTableName({type}, {15}, row_num, row_num);
        std::vector<int64_t> template_args{
            (type == type::TypeId::INTEGER) ? idx_key_size : 0, (type == type::TypeId::BIGINT) ? idx_key_size : 0,
            (type == type::TypeId::INTEGER) ? 15 : 0, (type == type::TypeId::BIGINT) ? 15 : 0, row_num};

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
          mapping[tbl].emplace_back(std::move(arg_vec));
        }

        for (auto lookup : lookups) {
          std::vector<int64_t> arg_vec{template_args};
          arg_vec.emplace_back(lookup);
          arg_vec.emplace_back(-1);
          mapping[tbl].emplace_back(std::move(arg_vec));
        }

        if (!lookups.empty()) {
          // Special argument used to indicate a drop index
          std::vector<int64_t> arg_vec{template_args};
          arg_vec.emplace_back(0);
          arg_vec.emplace_back(0);
          mapping[tbl].emplace_back(std::move(arg_vec));
        }
      }
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, this, mode, std::move(map.second));
  }
}

void MiniRunnerDeleteExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_bigints = iteration[1];
  auto tbl_ints = iteration[2];
  auto tbl_bigints = iteration[3];
  auto row = iteration[4];
  auto car = iteration[5];
  auto is_build = iteration[6];

  // A lookup size of 0 indicates a special query
  auto type = tbl_ints != 0 ? (type::TypeId::INTEGER) : (type::TypeId::BIGINT);
  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto bigint_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::BIGINT);
  auto tuple_size = int_size * num_integers + bigint_size * num_bigints;
  auto num_col = num_integers + num_bigints;
  auto tbl_col = tbl_ints + tbl_bigints;
  auto tbl_size = tbl_ints * int_size + tbl_bigints * bigint_size;

  if (car == 0) {
    // A lookup size of 0 indicates a special query
    if (is_build < 0) {
      throw "Invalid is_build argument for ExecuteDelete";
    }

    MiniRunnersExecUtil::HandleBuildDropIndex(*db_main_, settings_->db_oid_, is_build != 0, tbl_col, row, num_col,
                                              type);
    return;
  }

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::DELETE, car, tbl_size,
                         tbl_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::INDEX_DELETE, row,
                         tuple_size, num_col, car, 1, 0, 0);
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::IDX_SCAN, row,
                         tuple_size, num_col, car, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  std::string query_final;
  {
    std::stringstream query;
    std::string predicate = MiniRunnersSqlUtil::ConstructIndexScanPredicate(type, num_col, car);
    std::string tbl = MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_col, 0, row, row);
    query << "DELETE FROM " << tbl << " WHERE " << predicate;
    query_final = query.str();
  }

  int num_iters = 1 + (car <= settings_->warmup_rows_limit_ ? settings_->warmup_iterations_num_ : 0);
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
    optimize.checker =
        std::bind(MiniRunnersExecUtil::ChildIndexScanChecker, std::placeholders::_1, std::placeholders::_2);
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
