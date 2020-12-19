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
class MiniRunnersSqlUtil {
 public:
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
