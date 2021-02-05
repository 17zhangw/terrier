#pragma once

#include "catalog/catalog_defs.h"

namespace noisepage::transaction {
class TransactionContext;
}  // namespace noisepage::transaction

namespace noisepage::settings {
class SettingsManager;
}  // namespace noisepage::settings

namespace noisepage::catalog {
class CatalogAccessor;
}  // namespace noisepage::catalog

namespace noisepage::optimizer {
class AbstractCostModel;
class StatsStorage;
}  // namespace noisepage::optimizer

namespace noisepage::util {

/**
 * Utility class for query execution
 */
class QueryExecUtil {
 public:
  static bool ExecuteStatement(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                               common::ManagedPointer<catalog::CatalogAccessor> accessor,
                               common::ManagedPointer<settings::SettingsManager> settings,
                               std::unique_ptr<optimizer::AbstractCostModel> model,
                               common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                               const std::string &statement);
};

}  // namespace noisepage::util
