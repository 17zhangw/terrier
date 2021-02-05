#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"
#include "network/postgres/postgres_network_commands_util.h"
#include "optimizer/cost_model/trivial_cost_model.h"

namespace noisepage {

void DBMain::Run() {
  // Load startup ddls
  std::vector<std::string> startup_ddls;
  if (settings_manager_ != NULL) {
    auto input = settings_manager_->GetString(settings::Param::startup_ddl_path);
    std::ifstream ddl_file(input);
    if (ddl_file.is_open() && ddl_file.good()) {
      std::string input_line;
      while (std::getline(ddl_file, input_line)) {
        startup_ddls.emplace_back(std::move(input_line));
      }
    }
  }

  if (!startup_ddls.empty()) {
    auto txn_manager = txn_layer_->GetTransactionManager();
    auto catalog = catalog_layer_->GetCatalog();
    auto *txn = txn_manager->BeginTransaction();
    auto db_oid = catalog->GetDatabaseOid(common::ManagedPointer(txn), catalog::DEFAULT_DATABASE);
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    for (auto &ddl : startup_ddls) {
      util::QueryExecUtil::ExecuteStatement(db_oid, common::ManagedPointer(txn), common::ManagedPointer(accessor),
                                            common::ManagedPointer(settings_manager_),
                                            std::make_unique<optimizer::TrivialCostModel>(),
                                            common::ManagedPointer(stats_storage_),
                                            settings_manager_->GetInt(settings::Param::task_execution_timeout), ddl);
    }
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

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

}  // namespace noisepage
