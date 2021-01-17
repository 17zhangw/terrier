#include <random>

#include "binder/bind_node_visitor.h"
#include "execution/compiler/compilation_context.h"
#include "execution/sql/ddl_executors.h"
#include "optimizer/cost_model/abstract_cost_model.h"
#include "runner/mini_runners_exec_util.h"

namespace noisepage::runner {

void MiniRunnersExecUtil::DropIndexByName(DBMain *db_main, catalog::db_oid_t db_oid, const std::string &name) {
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  auto txn = txn_manager->BeginTransaction();
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  auto index_oid = accessor->GetIndexOid(name);
  accessor->DropIndex(index_oid);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

MiniRunnersExecUtil::OptimizeResult MiniRunnersExecUtil::OptimizeSqlStatement(struct OptimizeRequest *request) {
  auto *db_main = request->db_main;
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto txn = txn_manager->BeginTransaction();
  auto stmt_list = parser::PostgresParser::BuildParseTree(request->query);

  auto db_oid = request->db_oid;
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto binder = binder::BindNodeVisitor(common::ManagedPointer(accessor), db_oid);
  binder.BindNameToNode(common::ManagedPointer(stmt_list), request->params, request->param_types);

  auto out_plan = trafficcop::TrafficCopUtil::Optimize(
                      common::ManagedPointer(txn), common::ManagedPointer(accessor), common::ManagedPointer(stmt_list),
                      db_oid, db_main->GetStatsStorage(), std::move(request->cost_model), 1000000)
                      ->TakePlanNodeOwnership();

  if (request->checker != nullptr) {
    request->checker(common::ManagedPointer(txn), out_plan.get());
  }

  // Handle DDL cases
  if (out_plan->GetPlanNodeType() == planner::PlanNodeType::CREATE_INDEX) {
    execution::sql::DDLExecutors::CreateIndexExecutor(
        common::ManagedPointer<planner::CreateIndexPlanNode>(
            reinterpret_cast<planner::CreateIndexPlanNode *>(out_plan.get())),
        common::ManagedPointer<catalog::CatalogAccessor>(accessor));
  }

  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      db_oid, common::ManagedPointer(txn), execution::exec::NoOpResultConsumer(), out_plan->GetOutputSchema().Get(),
      common::ManagedPointer(accessor), request->exec_settings, db_main->GetMetricsManager());

  auto exec_query = execution::compiler::CompilationContext::Compile(*out_plan, request->exec_settings, accessor.get(),
                                                                     execution::compiler::CompilationMode::OneShot);

  // Some runners rely on counters to work correctly (i.e parallel create index).
  // Counter code relies on the ids of features extracted during code generation
  // to update numbers. However, the synthetic pipeline + features that are
  // set by the mini-runners do not have these feature ids available.
  //
  // For now, since a pipeline does not contain any duplicate feature types (i.e
  // there will not be 2 hashjoin_probes in 1 pipeline), we can assign feature ids
  // to our synthetic features by finding the matching feature from the feature
  // vector produced during codegen.
  auto pipeline = exec_query->GetPipelineOperatingUnits();
  for (auto &info : request->pipeline_units->units_) {
    auto other_feature = pipeline->units_[info.first];
    for (auto &oufeature : info.second) {
      for (const auto &other_oufeature : other_feature) {
        if (oufeature.GetExecutionOperatingUnitType() == other_oufeature.GetExecutionOperatingUnitType()) {
          oufeature.feature_id_ = other_oufeature.feature_id_;
          break;
        }
      }
    }
  }

  exec_query->SetPipelineOperatingUnits(std::move(request->pipeline_units));

  auto ret_val = std::make_pair(std::move(exec_query), out_plan->GetOutputSchema()->Copy());
  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  return ret_val;
}

void MiniRunnersExecUtil::ExecuteQuery(struct ExecuteRequest *request) {
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
