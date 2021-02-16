#pragma once

#include "catalog/catalog_defs.h"
#include "execution/sql/value.h"

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

namespace noisepage::planner {
class AbstractPlanNode;
}  // namespace noisepage::planner

namespace noisepage::network {
class Statement;
}  // namespace noisepage::network

namespace noisepage::util {

using TupleFunction = std::function<void(const std::vector<execution::sql::Val *> &)>;

/**
 * Utility class for query execution
 */
class QueryExecUtil {
 public:
  static std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> PlanStatement(
      catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
      common::ManagedPointer<catalog::CatalogAccessor> accessor, std::unique_ptr<optimizer::AbstractCostModel> model,
      common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout, const std::string &statement);

  static bool ExecuteDDL(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                         common::ManagedPointer<catalog::CatalogAccessor> accessor,
                         common::ManagedPointer<settings::SettingsManager> settings,
                         std::unique_ptr<optimizer::AbstractCostModel> model,
                         common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                         const std::string &statement);

  static bool ExecuteDML(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                         common::ManagedPointer<catalog::CatalogAccessor> accessor,
                         common::ManagedPointer<settings::SettingsManager> settings,
                         std::unique_ptr<optimizer::AbstractCostModel> model,
                         common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                         const std::string &statement, TupleFunction tuple_fn);
};

}  // namespace noisepage::util
