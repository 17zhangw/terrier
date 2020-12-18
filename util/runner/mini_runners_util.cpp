#include <random>

#include "runner/mini_runners_util.h"

namespace noisepage::runner {

std::string MiniRunnersUtil::ConstructSQLClause(type::TypeId left_type, type::TypeId right_type, int64_t num_left,
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

std::string MiniRunnersUtil::ConstructIndexScanPredicate(type::TypeId key_type, int64_t key_num, int64_t lookup_size) {
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

void MiniRunnersUtil::GenIdxScanParameters(type::TypeId type_param, int64_t num_rows, int64_t lookup_size,
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

void MiniRunnersUtil::ExecuteQuery(struct ExecuteRequest *request) {
  auto *db_main = request->db_main_;
  auto *exec_query = request->exec_query_;

  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  transaction::TransactionContext *txn = nullptr;
  std::unique_ptr<catalog::CatalogAccessor> accessor = nullptr;

  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;
  auto num_iters = request->num_iters_;
  for (auto i = 0; i < num_iters; i++) {
    common::ManagedPointer<metrics::MetricsManager> metrics_manager = nullptr;
    if (i == num_iters - 1) {
      db_main->GetMetricsManager()->RegisterThread();
      metrics_manager = db_main->GetMetricsManager();
    }

    txn = txn_manager->BeginTransaction();
    accessor = catalog->GetAccessor(common::ManagedPointer(txn), request->db_oid_, DISABLED);

    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        request->db_oid_, common::ManagedPointer(txn), callback, request->out_schema_, common::ManagedPointer(accessor),
        request->exec_settings_, metrics_manager);

    // Attach params to ExecutionContext
    if (static_cast<size_t>(i) < request->params_.size()) {
      exec_ctx->SetParams(
          common::ManagedPointer<const std::vector<parser::ConstantValueExpression>>(&request->params_[i]));
    }

    exec_query->Run(common::ManagedPointer(exec_ctx), request->mode_);

    NOISEPAGE_ASSERT(!txn->MustAbort(), "Transaction should not be force-aborted");
    if (request->commit_)
      txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    else
      txn_manager->Abort(txn);

    if (i == num_iters - 1) {
      metrics_manager->Aggregate();
      metrics_manager->UnregisterThread();
    }
  }
}

};  // namespace noisepage::runner
