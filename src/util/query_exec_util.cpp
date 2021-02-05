#include "util/query_exec_util.h"

#include "binder/bind_node_visitor.h"
#include "catalog/catalog_accessor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/compiler/executable_query.h"
#include "execution/exec/execution_context.h"
#include "execution/sql/ddl_executors.h"
#include "execution/vm/vm_defs.h"
#include "metrics/metrics_manager.h"
#include "network/network_defs.h"
#include "network/network_util.h"
#include "network/postgres/statement.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "optimizer/statistics/stats_storage.h"
#include "parser/postgresparser.h"
#include "parser/variable_set_statement.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_context.h"

namespace noisepage::util {

bool QueryExecUtil::ExecuteStatement(catalog::db_oid_t db_oid,
                                     common::ManagedPointer<transaction::TransactionContext> txn,
                                     common::ManagedPointer<catalog::CatalogAccessor> accessor,
                                     common::ManagedPointer<settings::SettingsManager> settings,
                                     std::unique_ptr<optimizer::AbstractCostModel> model,
                                     common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                                     const std::string &query) {
  std::unique_ptr<network::Statement> statement;
  try {
    std::string query_tmp = query;
    auto parse_tree = parser::PostgresParser::BuildParseTree(query_tmp);
    statement = std::make_unique<network::Statement>(std::move(query_tmp), std::move(parse_tree));
  } catch (std::exception &e) {
    // Catched a parsing error
    return false;
  }

  // Handle SET queries
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    settings->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
    return true;
  }

  try {
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(statement->ParseResult(), nullptr, nullptr);
  } catch (std::exception &e) {
    // Caught a binding exception
    return false;
  }

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(txn, accessor, statement->ParseResult(), db_oid, stats,
                                                       std::move(model), optimizer_timeout)
                      ->TakePlanNodeOwnership();

  if (out_plan == NULL) {
    // Failed an optimization pass
    return false;
  }

  if (network::NetworkUtil::DMLQueryType(statement->GetQueryType())) {
    // Setup execution settings and callback
    execution::exec::ExecutionSettings exec_settings{};
    exec_settings.UpdateFromSettingsManager(settings);
    execution::exec::NoOpResultConsumer consumer;
    execution::exec::OutputCallback callback = consumer;

    // Create ExecutionContext with no metrics to prevent recording
    common::ManagedPointer<metrics::MetricsManager> metrics(nullptr);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        db_oid, txn, callback, out_plan->GetOutputSchema().Get(), accessor, exec_settings, metrics);

    auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings, accessor.Get(),
                                                                       execution::compiler::CompilationMode::OneShot);
    exec_query->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);
    return true;
  }

  switch (statement->GetQueryType()) {
    case network::QueryType::QUERY_CREATE_TABLE:
      return execution::sql::DDLExecutors::CreateTableExecutor(
          common::ManagedPointer<planner::CreateTablePlanNode>(
              reinterpret_cast<planner::CreateTablePlanNode *>(out_plan.get())),
          accessor, db_oid);
    default:
      NOISEPAGE_ASSERT(false, "Unsupported QueryExecUtil::ExecuteStatement");
      break;
  }

  return false;
}

}  // namespace noisepage::util
