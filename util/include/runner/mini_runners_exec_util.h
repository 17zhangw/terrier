#pragma once

#include "common/action_context.h"
#include "execution/exec/execution_settings.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "parser/expression/constant_value_expression.h"
#include "settings/settings_manager.h"
#include "settings/settings_param.h"
#include "type/type_id.h"

namespace noisepage::runner {

/**
 * Collection of utility functions for the runners
 */
class MiniRunnersExecUtil {
 public:
  static execution::exec::ExecutionSettings GetExecutionSettings(bool pipeline_metrics_enabled) {
    execution::exec::ExecutionSettings exec_settings;
    exec_settings.is_parallel_execution_enabled_ = false;
    exec_settings.is_counters_enabled_ = false;
    exec_settings.is_pipeline_metrics_enabled_ = pipeline_metrics_enabled;
    return exec_settings;
  }

  static execution::exec::ExecutionSettings GetParallelExecutionSettings(size_t num_threads, bool counters) {
    execution::exec::ExecutionSettings exec_settings;
    exec_settings.is_pipeline_metrics_enabled_ = true;
    exec_settings.is_parallel_execution_enabled_ = (num_threads != 0);
    exec_settings.number_of_parallel_execution_threads_ = num_threads;
    exec_settings.is_counters_enabled_ = counters;
    exec_settings.is_static_partitioner_enabled_ = true;
    return exec_settings;
  }

  template <settings::Param param, typename T>
  static void DbMainSetParam(DBMain *db_main, T value) {
    const common::action_id_t action_id(1);
    auto callback = [](common::ManagedPointer<common::ActionContext> action UNUSED_ATTRIBUTE) {};
    settings::setter_callback_fn setter_callback = callback;
    auto db_settings = db_main->GetSettingsManager();

    if (std::is_same<T, bool>::value) {
      auto action_context = std::make_unique<common::ActionContext>(action_id);
      db_settings->SetBool(param, value, common::ManagedPointer(action_context), setter_callback);
    } else if (std::is_integral<T>::value) {
      auto action_context = std::make_unique<common::ActionContext>(action_id);
      db_settings->SetInt(param, value, common::ManagedPointer(action_context), setter_callback);
    }
  }

  static void HandleBuildDropIndex(DBMain *db_main, catalog::db_oid_t db_oid, bool is_build, int64_t tbl_cols,
                                   int64_t num_rows, int64_t num_key, type::TypeId type);
  static void DropIndexByName(DBMain *db_main, catalog::db_oid_t db_oid, const std::string &name);

  struct OptimizeRequest {
    DBMain *db_main;
    catalog::db_oid_t db_oid;
    std::string query;
    std::unique_ptr<optimizer::AbstractCostModel> cost_model;
    std::unique_ptr<selfdriving::PipelineOperatingUnits> pipeline_units;
    std::function<void(common::ManagedPointer<transaction::TransactionContext>, planner::AbstractPlanNode *)> checker;
    common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params = nullptr;
    common::ManagedPointer<std::vector<type::TypeId>> param_types = nullptr;
    execution::exec::ExecutionSettings exec_settings;
  };

  using OptimizeResult =
      std::pair<std::unique_ptr<execution::compiler::ExecutableQuery>, std::unique_ptr<planner::OutputSchema>>;
  static OptimizeResult OptimizeSqlStatement(struct OptimizeRequest *request);

  struct ExecuteRequest {
    DBMain *db_main_;
    catalog::db_oid_t db_oid_;
    execution::compiler::ExecutableQuery *exec_query_;
    planner::OutputSchema *out_schema_;
    int64_t num_iters_;
    bool commit_;
    execution::vm::ExecutionMode mode_;
    execution::exec::ExecutionSettings exec_settings_;
    std::vector<std::vector<parser::ConstantValueExpression>> params_;
  };

  static void ExecuteQuery(struct ExecuteRequest *request);
};

};  // namespace noisepage::runner
