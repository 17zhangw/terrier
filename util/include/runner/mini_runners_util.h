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
class MiniRunnersUtil {
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

  static std::string ConstructTableName(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
                                        int64_t num_right, size_t row, size_t car) {
    std::vector<type::TypeId> types = {left_type, right_type};
    std::vector<uint32_t> col_counts = {static_cast<uint32_t>(num_left), static_cast<uint32_t>(num_right)};
    return execution::sql::TableGenerator::GenerateTableName(types, col_counts, row, car);
  }

  /**
   * Construct a SQL clause.
   *
   * If alias is non-empty, then generates [left_alias].col# (similar for right_alias).
   * right_alias is only used if generating a predicate.
   * joiner is the string used for joining column statements together.
   *
   * is_predicate describes whether it's generating a predicate or just a projection.
   */
  static std::string ConstructSQLClause(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
                                        int64_t num_right, const std::string &joiner, const std::string &left_alias,
                                        bool is_predicate, const std::string &right_alias);

  static std::string ConstructIndexScanPredicate(type::TypeId key_type, int64_t key_num, int64_t lookup_size);

  static void GenIdxScanParameters(type::TypeId type_param, int64_t num_rows, int64_t lookup_size, int64_t num_iters,
                                   std::vector<std::vector<parser::ConstantValueExpression>> *real_params);
};

};  // namespace noisepage::runner
