#include <random>

#include "runner/mini_runners_sql_util.h"

namespace noisepage::runner {

std::string MiniRunnersSqlUtil::ConstructSQLClause(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
                                                   int64_t num_right, const std::string &joiner,
                                                   const std::string &left_alias, bool is_predicate,
                                                   const std::string &right_alias) {
  std::stringstream fragment;

  std::vector<type::TypeId> types = {left_type, right_type};
  std::vector<int64_t> number = {num_left, num_right};
  bool emit_alias = !left_alias.empty();
  bool wrote = false;
  for (size_t i = 0; i < types.size(); i++) {
    if (types[i] == type::TypeId::INVALID) {
      // Skip invalid types (that means don't care)
      continue;
    }

    auto type = type::TypeUtil::TypeIdToString(types[i]);
    for (auto col = 1; col <= number[i]; col++) {
      if (wrote) {
        fragment << joiner;
      }

      if (emit_alias) {
        fragment << left_alias << ".";
      }

      fragment << type << col;
      if (is_predicate) {
        fragment << " = ";
        if (!right_alias.empty()) {
          fragment << right_alias << ".";
        }
        fragment << type << col;
      }

      wrote = true;
    }
  }
  return fragment.str();
}

std::string MiniRunnersSqlUtil::ConstructIndexScanPredicate(type::TypeId key_type, int64_t key_num,
                                                            int64_t lookup_size) {
  auto type = type::TypeUtil::TypeIdToString(key_type);
  std::stringstream predicatess;
  for (auto j = 1; j <= key_num; j++) {
    if (lookup_size == 1) {
      predicatess << type << j << " = $1";
    } else {
      predicatess << type << j << " >= $1";
      predicatess << " AND " << type << j << " <= $2";
    }

    if (j != key_num) predicatess << " AND ";
  }
  return predicatess.str();
}

void MiniRunnersSqlUtil::GenIdxScanParameters(type::TypeId type_param, int64_t num_rows, int64_t lookup_size,
                                              int64_t num_iters,
                                              std::vector<std::vector<parser::ConstantValueExpression>> *real_params) {
  std::mt19937 generator{};
  std::vector<std::pair<uint32_t, uint32_t>> bounds;
  for (int i = 0; i < num_iters; i++) {
    // Pick a range [0, 10). The span is num_rows - lookup_size / 10 which controls
    // the span of numbers within a given range.
    int num_regions = 10;
    int64_t span = (num_rows - lookup_size) / num_regions;
    auto range =
        std::uniform_int_distribution(static_cast<uint32_t>(0), static_cast<uint32_t>(num_regions - 1))(generator);
    auto low_key = std::uniform_int_distribution(static_cast<uint32_t>(0),
                                                 static_cast<uint32_t>((span >= 1) ? (span - 1) : 0))(generator);
    low_key += range * span;

    std::vector<parser::ConstantValueExpression> param;
    if (lookup_size == 1) {
      if (type_param != type::TypeId::VARCHAR) {
        param.emplace_back(type_param, execution::sql::Integer(low_key));
      } else {
        std::string val = std::to_string(low_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
      }
      bounds.emplace_back(low_key, low_key);
    } else {
      auto high_key = low_key + lookup_size - 1;
      if (type_param != type::TypeId::VARCHAR) {
        param.emplace_back(type_param, execution::sql::Integer(low_key));
        param.emplace_back(type_param, execution::sql::Integer(high_key));
      } else {
        std::string val = std::to_string(low_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
        val = std::to_string(high_key);
        param.emplace_back(type_param, execution::sql::StringVal(val.c_str()));
      }
      bounds.emplace_back(low_key, high_key);
    }

    real_params->emplace_back(std::move(param));
  }
}

void MiniRunnersSqlUtil::GenMixedArguments(std::vector<std::vector<int64_t>> *args, const MiniRunnersSettings &settings,
                                           const MiniRunnersDataConfig &config, const std::vector<uint32_t> &row_nums,
                                           uint32_t varchar_mix) {
  std::vector<std::pair<uint32_t, uint32_t>> mixed_dist;
  uint32_t step_size;
  if (varchar_mix == 0) {
    /* Vector of table distributions <INTEGER, DECIMALS> */
    mixed_dist = config.sweep_scan_mixed_dist_;
    step_size = 2;
  } else {
    /* Vector of table distributions <INTEGER, VARCHAR> */
    mixed_dist = config.sweep_scan_mixed_varchar_dist_;
    step_size = 1;
  } /* Always generate full table scans for all row_num and cardinalities. */
  for (auto col_dist : mixed_dist) {
    std::pair<uint32_t, uint32_t> start = {col_dist.first - step_size, step_size};
    while (true) {
      for (auto row : row_nums) {
        int64_t car = 1;
        while (car < row) {
          args->push_back({start.first, start.second, col_dist.first, col_dist.second, row, car, varchar_mix});
          car *= 2;
        }
        args->push_back({start.first, start.second, col_dist.first, col_dist.second, row, row, varchar_mix});
      }
      if (start.second < col_dist.second) {
        start.second += step_size;
      } else if (start.first < col_dist.first) {
        start.first += step_size;
        start.second = step_size;
      } else {
        break;
      }
    }
  }
}

};  // namespace noisepage::runner
