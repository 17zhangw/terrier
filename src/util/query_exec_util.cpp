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

std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> QueryExecUtil::PlanStatement(
    catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
    common::ManagedPointer<catalog::CatalogAccessor> accessor, std::unique_ptr<optimizer::AbstractCostModel> model,
    common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout, const std::string &query) {
  std::unique_ptr<network::Statement> statement;
  try {
    std::string query_tmp = query;
    auto parse_tree = parser::PostgresParser::BuildParseTree(query_tmp);
    statement = std::make_unique<network::Statement>(std::move(query_tmp), std::move(parse_tree));
  } catch (std::exception &e) {
    // Catched a parsing error
    return {nullptr, nullptr};
  }

  try {
    // TODO(wz2): Specify params?
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
    binder.BindNameToNode(statement->ParseResult(), nullptr, nullptr);
  } catch (std::exception &e) {
    // Caught a binding exception
    return {nullptr, nullptr};
  }

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(txn, accessor, statement->ParseResult(), db_oid, stats,
                                                       std::move(model), optimizer_timeout)
                      ->TakePlanNodeOwnership();
  return std::make_pair(std::move(statement), std::move(out_plan));
}

bool QueryExecUtil::ExecuteDDL(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                               common::ManagedPointer<catalog::CatalogAccessor> accessor,
                               common::ManagedPointer<settings::SettingsManager> settings,
                               std::unique_ptr<optimizer::AbstractCostModel> model,
                               common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                               const std::string &query) {
  auto result = PlanStatement(db_oid, txn, accessor, std::move(model), stats, optimizer_timeout, query);
  const std::unique_ptr<network::Statement> &statement = result.first;
  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(!network::NetworkUtil::DMLQueryType(statement->GetQueryType()), "ExecuteDDL expects DDL statement");

  // Handle SET queries
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    settings->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
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

bool QueryExecUtil::ExecuteDML(catalog::db_oid_t db_oid, common::ManagedPointer<transaction::TransactionContext> txn,
                               common::ManagedPointer<catalog::CatalogAccessor> accessor,
                               common::ManagedPointer<settings::SettingsManager> settings,
                               std::unique_ptr<optimizer::AbstractCostModel> model,
                               common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout,
                               const std::string &query, TupleFunction tuple_fn) {
  auto result = PlanStatement(db_oid, txn, accessor, std::move(model), stats, optimizer_timeout, query);
  const std::unique_ptr<network::Statement> &statement = result.first;
  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(network::NetworkUtil::DMLQueryType(statement->GetQueryType()), "ExecuteDML expects DML");
  common::ManagedPointer<planner::OutputSchema> schema = out_plan->GetOutputSchema();

  auto consumer = [&tuple_fn, schema](byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
    for (uint32_t row = 0; row < num_tuples; row++) {
      uint32_t curr_offset = 0;
      std::vector<execution::sql::Val *> vals;
      for (const auto &col : schema->GetColumns()) {
        auto alignment = execution::sql::ValUtil::GetSqlAlignment(col.GetType());
        if (!common::MathUtil::IsAligned(curr_offset, alignment)) {
          curr_offset = static_cast<uint32_t>(common::MathUtil::AlignTo(curr_offset, alignment));
        }

        auto *val = reinterpret_cast<execution::sql::Val *>(tuples + row * tuple_size + curr_offset);
        vals.emplace_back(val);
        curr_offset += execution::sql::ValUtil::GetSqlSize(col.GetType());
      }

      tuple_fn(vals);
    }
  };

  // Setup execution settings and callback
  execution::exec::ExecutionSettings exec_settings{};
  exec_settings.UpdateFromSettingsManager(settings);
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

}  // namespace noisepage::util
