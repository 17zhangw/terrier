#include "util/query_exec_util.h"
#include "binder/bind_node_visitor.h"
#include "catalog/catalog.h"
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
#include "parser/expression/constant_value_expression.h"
#include "parser/postgresparser.h"
#include "parser/variable_set_statement.h"
#include "settings/settings_manager.h"
#include "transaction/transaction_context.h"
#include "transaction/transaction_manager.h"

namespace noisepage::util {

QueryExecUtil::QueryExecUtil(catalog::db_oid_t db_oid,
                             common::ManagedPointer<transaction::TransactionManager> txn_manager,
                             common::ManagedPointer<catalog::Catalog> catalog,
                             common::ManagedPointer<settings::SettingsManager> settings,
                             common::ManagedPointer<optimizer::StatsStorage> stats, uint64_t optimizer_timeout)
    : db_oid_(db_oid),
      txn_manager_(txn_manager),
      catalog_(catalog),
      settings_(settings),
      stats_(stats),
      optimizer_timeout_(optimizer_timeout) {
  exec_settings_ = execution::exec::ExecutionSettings{};
  exec_settings_.UpdateFromSettingsManager(settings_);
}

void QueryExecUtil::SetDatabase(catalog::db_oid_t db_oid) { db_oid_ = db_oid; }

void QueryExecUtil::SetExecutionSettings(execution::exec::ExecutionSettings exec_settings) {
  exec_settings_ = exec_settings;
}

void QueryExecUtil::ClearPlans() {
  schemas_.clear();
  exec_queries_.clear();
}

void QueryExecUtil::UseTransaction(common::ManagedPointer<transaction::TransactionContext> txn) {
  NOISEPAGE_ASSERT(txn_ != NULL, "Nesting transactions not supported");
  txn_ = txn.Get();
  own_txn_ = false;
}

void QueryExecUtil::BeginTransaction() {
  NOISEPAGE_ASSERT(txn_ != NULL, "Nesting transactions not supported");
  txn_ = txn_manager_->BeginTransaction();
  own_txn_ = true;
}

void QueryExecUtil::EndTransaction(bool commit) {
  if (own_txn_) {
    if (commit)
      txn_manager_->Commit(txn_, transaction::TransactionUtil::EmptyCallback, nullptr);
    else
      txn_manager_->Abort(txn_);
  }
  txn_ = nullptr;
}

void QueryExecUtil::SetCostModelFunction(std::function<std::unique_ptr<optimizer::AbstractCostModel>()> func) {
  cost_func_ = func;
}

std::pair<std::unique_ptr<network::Statement>, std::unique_ptr<planner::AbstractPlanNode>> QueryExecUtil::PlanStatement(
    const std::string &query, common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
    common::ManagedPointer<std::vector<type::TypeId>> param_types) {
  NOISEPAGE_ASSERT(txn_ != nullptr, "Transaction must have been started");
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto model = cost_func_();

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
    auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid_);
    binder.BindNameToNode(statement->ParseResult(), params, param_types);
  } catch (std::exception &e) {
    // Caught a binding exception
    return {nullptr, nullptr};
  }

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(txn, common::ManagedPointer(accessor), statement->ParseResult(),
                                                       db_oid_, stats_, std::move(model), optimizer_timeout_)
                      ->TakePlanNodeOwnership();
  return std::make_pair(std::move(statement), std::move(out_plan));
}

bool QueryExecUtil::ExecuteDDL(const std::string &query) {
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  bool require_commit = false;
  if (txn_ == nullptr) {
    require_commit = true;
    BeginTransaction();
    txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  }

  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto result = PlanStatement(query, nullptr, nullptr);
  const std::unique_ptr<network::Statement> &statement = result.first;
  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(!network::NetworkUtil::DMLQueryType(statement->GetQueryType()), "ExecuteDDL expects DDL statement");

  // Handle SET queries
  bool status = true;
  if (statement->GetQueryType() == network::QueryType::QUERY_SET) {
    const auto &set_stmt = statement->RootStatement().CastManagedPointerTo<parser::VariableSetStatement>();
    settings_->SetParameter(set_stmt->GetParameterName(), set_stmt->GetValues());
    status = true;
  } else {
    switch (statement->GetQueryType()) {
      case network::QueryType::QUERY_CREATE_TABLE:
        status = execution::sql::DDLExecutors::CreateTableExecutor(
            common::ManagedPointer<planner::CreateTablePlanNode>(
                reinterpret_cast<planner::CreateTablePlanNode *>(out_plan.get())),
            common::ManagedPointer(accessor), db_oid_);
        break;
      default:
        NOISEPAGE_ASSERT(false, "Unsupported QueryExecUtil::ExecuteStatement");
        break;
    }
  }

  if (require_commit) {
    // Commit if success
    EndTransaction(status);
  }

  return status;
}

size_t QueryExecUtil::CompileQuery(const std::string &statement,
                                   common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                                   common::ManagedPointer<std::vector<type::TypeId>> param_types) {
  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  bool require_commit = false;
  if (txn == nullptr) {
    require_commit = true;
    BeginTransaction();
    txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  }

  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto result = PlanStatement(statement, params, param_types);
  const std::unique_ptr<network::Statement> &network_statement = result.first;
  const std::unique_ptr<planner::AbstractPlanNode> &out_plan = result.second;
  NOISEPAGE_ASSERT(network::NetworkUtil::DMLQueryType(network_statement->GetQueryType()), "ExecuteDML expects DML");
  common::ManagedPointer<planner::OutputSchema> schema = out_plan->GetOutputSchema();

  auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, exec_settings_, accessor.get(),
                                                                     execution::compiler::CompilationMode::OneShot);
  schemas_.push_back(schema->Copy());
  exec_queries_.push_back(std::move(exec_query));

  if (require_commit) {
    EndTransaction(false);
  }
  return exec_queries_.size() - 1;
}

bool QueryExecUtil::ExecuteQuery(size_t idx, TupleFunction tuple_fn,
                                 common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                                 common::ManagedPointer<metrics::MetricsManager> metrics) {
  NOISEPAGE_ASSERT(idx < exec_queries_.size(), "Invalid query index");

  auto txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  bool require_commit = false;
  if (txn == nullptr) {
    require_commit = true;
    BeginTransaction();
    txn = common::ManagedPointer<transaction::TransactionContext>(txn_);
  }

  planner::OutputSchema *schema = schemas_[idx].get();
  auto consumer = [&tuple_fn, schema](byte *tuples, uint32_t num_tuples, uint32_t tuple_size) {
    if (tuple_fn != nullptr) {
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
    }
  };

  // Create ExecutionContext with no metrics to prevent recording
  execution::exec::OutputCallback callback = consumer;
  auto accessor = catalog_->GetAccessor(txn, db_oid_, DISABLED);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid_, txn, callback, schema, common::ManagedPointer(accessor), exec_settings_, metrics);

  exec_queries_[idx]->Run(common::ManagedPointer(exec_ctx), execution::vm::ExecutionMode::Interpret);

  if (require_commit) {
    EndTransaction(true);
  }

  return true;
}

bool QueryExecUtil::ExecuteDML(const std::string &query,
                               common::ManagedPointer<std::vector<parser::ConstantValueExpression>> params,
                               common::ManagedPointer<std::vector<type::TypeId>> param_types, TupleFunction tuple_fn,
                               common::ManagedPointer<metrics::MetricsManager> metrics) {
  size_t idx = CompileQuery(query, params, param_types);
  return ExecuteQuery(idx, tuple_fn, params, metrics);
}

}  // namespace noisepage::util