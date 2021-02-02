#include "main/db_main.h"

#define __SETTING_GFLAGS_DEFINE__    // NOLINT
#include "settings/settings_defs.h"  // NOLINT
#undef __SETTING_GFLAGS_DEFINE__     // NOLINT

#include "execution/execution_util.h"
#include "network/loopback/loopback.h"
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
    network::LoopbackConnection loopback{settings_manager_->GetInt(settings::Param::port)};
    if (loopback.Connect(catalog::DEFAULT_DATABASE)) {
      loopback.ExecuteDDLs(startup_ddls);
    }
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
