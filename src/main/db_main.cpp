#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"
#include "network/postgres/postgres_network_commands_util.h"

namespace noisepage {

void DBMain::Run() {
  NOISEPAGE_ASSERT(network_layer_ != DISABLED, "Trying to run without a NetworkLayer.");
  const auto server = network_layer_->GetServer();
  try {
    server->RunServer();
  } catch (NetworkProcessException &e) {
    return;
  }
  {
    std::unique_lock<std::mutex> lock(server->RunningMutex());
    server->RunningCV().wait(lock, [=] { return !(server->Running()); });
  }
}

void DBMain::ForceShutdown() {
  if (network_layer_ != DISABLED && network_layer_->GetServer()->Running()) {
    network_layer_->GetServer()->StopServer();
  }
}

DBMain::~DBMain() { ForceShutdown(); }

DBMain::ExecutionLayer::ExecutionLayer() { execution::ExecutionUtil::InitTPL(); }

DBMain::ExecutionLayer::~ExecutionLayer() { execution::ExecutionUtil::ShutdownTPL(); }

void DBMain::Builder::LoadStartupDDL(common::ManagedPointer<settings::SettingsManager> settings,
                                     common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                     common::ManagedPointer<catalog::Catalog> catalog,
                                     common::ManagedPointer<transaction::TransactionManager> txn_manager,
                                     std::string db_name) {
  auto input = settings->GetString(settings::Param::startup_ddl_path);
  std::ifstream ddl_file(input);
  if (!ddl_file.is_open() || !ddl_file.good()) {
    return;
  }

  std::string input_line;
  auto queue = std::make_unique<network::WriteQueue>();
  auto out = std::make_unique<network::PostgresPacketWriter>(common::ManagedPointer(queue));

  auto connection = std::make_unique<network::ConnectionContext>();
  {
    auto *txn = txn_manager->BeginTransaction();
    auto db_oid = catalog->GetDatabaseOid(common::ManagedPointer(txn), db_name);
    if (db_oid == catalog::INVALID_DATABASE_OID) {
      txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
      throw ABORT_EXCEPTION(fmt::format("Startup encountered error resolving {} database", db_name));
    }
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
    connection->SetDatabaseName(std::move(db_name));
    connection->SetDatabaseOid(db_oid);
  }

  while (std::getline(ddl_file, input_line)) {
    std::string query_text = input_line;
    auto parse_result = t_cop->ParseQuery(query_text, common::ManagedPointer(connection));
    if (std::holds_alternative<common::ErrorData>(parse_result)) {
      throw ABORT_EXCEPTION(fmt::format("Error encountered while processing: {}", input_line));
    }

    const auto statement = std::make_unique<network::Statement>(
        std::move(query_text), std::move(std::get<std::unique_ptr<parser::ParseResult>>(parse_result)));
    if (statement->Empty()) {
      continue;
    }

    t_cop->BeginTransaction(common::ManagedPointer(connection));
    const auto bind_result =
        t_cop->BindQuery(common::ManagedPointer(connection), common::ManagedPointer(statement), nullptr);
    if (bind_result.type_ == trafficcop::ResultType::COMPLETE) {
      // Binding succeeded, optimize to generate a physical plan and then execute
      auto optimize_result = t_cop->OptimizeBoundQuery(common::ManagedPointer(connection), statement->ParseResult());
      statement->SetOptimizeResult(std::move(optimize_result));

      const auto portal = std::make_unique<network::Portal>(common::ManagedPointer(statement));
      network::PostgresNetworkCommandsUtil::ExecutePortal(common::ManagedPointer(connection),
                                                          common::ManagedPointer(portal), common::ManagedPointer(out),
                                                          t_cop, false);
    } else {
      t_cop->EndTransaction(common::ManagedPointer(connection), network::QueryType::QUERY_ROLLBACK);
      throw ABORT_EXCEPTION(fmt::format("Error encountered while processing: {}", input_line));
    }

    t_cop->EndTransaction(common::ManagedPointer(connection), network::QueryType::QUERY_COMMIT);
  }
}

}  // namespace noisepage
