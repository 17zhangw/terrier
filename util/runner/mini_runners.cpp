#include "common/resource_tracker.h"
#include "execution/execution_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "runner/mini_runners_data_config.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_scheduler.h"
#include "runner/mini_runners_settings.h"

namespace noisepage::runner {

/**
 * MiniRunners Settings
 */
MiniRunnersSettings settings;

/**
 * MiniRunners Config
 */
MiniRunnersDataConfig config;

/**
 * Static db_main instance
 * This is done so all benchmarks reuse the same DB Main instance
 */
DBMain *db_main = nullptr;

void InvokeGC() {
  // Perform GC to do any cleanup
  auto gc = db_main->GetStorageLayer()->GetGarbageCollector();
  gc->PerformGarbageCollection();
  gc->PerformGarbageCollection();
}

/**
 * Mini-Runner number of executors
 */
#define NUM_EXECUTORS (17)

/**
 * Mini-Runner executors
 */
MiniRunnerExecutor *executors[NUM_EXECUTORS] = {new MiniRunnerArithmeticExecutor(&config, &settings, &db_main),
                                                new MiniRunnerOutputExecutor(&config, &settings, &db_main),
                                                new MiniRunnerNetworkOutputExecutor(&config, &settings, &db_main),
                                                new MiniRunnerSeqScanExecutor(&config, &settings, &db_main),
                                                new MiniRunnerIndexScanExecutor(&config, &settings, &db_main),
                                                new MiniRunnerSortExecutor(&config, &settings, &db_main),
                                                new MiniRunnerHashJoinNonSelfExecutor(&config, &settings, &db_main),
                                                new MiniRunnerHashJoinSelfExecutor(&config, &settings, &db_main),
                                                new MiniRunnerIndexJoinExecutor(&config, &settings, &db_main),
                                                new MiniRunnerAggKeyExecutor(&config, &settings, &db_main),
                                                new MiniRunnerAggKeylessExecutor(&config, &settings, &db_main),
                                                new MiniRunnerInsertExecutor(&config, &settings, &db_main),
                                                new MiniRunnerUpdateExecutor(&config, &settings, &db_main),
                                                new MiniRunnerDeleteExecutor(&config, &settings, &db_main),
                                                new MiniRunnerIndexInsertExecutor(&config, &settings, &db_main),
                                                new MiniRunnerIndexDeleteExecutor(&config, &settings, &db_main),
                                                new MiniRunnerCreateIndexExecutor(&config, &settings, &db_main)};

void InitializeRunnersState() {
  // Initialize parameter map and adjust necessary parameters
  std::unordered_map<settings::Param, settings::ParamInfo> param_map;
  settings::SettingsManager::ConstructParamMap(param_map);

  // Set this limit for the case of large updates/tables
  size_t limit = 1000000000;
  auto sql_val = execution::sql::Integer(limit);
  auto sql_false = execution::sql::BoolVal(false);
  param_map.find(settings::Param::block_store_size)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::block_store_reuse)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::record_buffer_segment_size)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::record_buffer_segment_reuse)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::INTEGER, sql_val);
  param_map.find(settings::Param::block_store_size)->second.max_value_ = limit;
  param_map.find(settings::Param::block_store_reuse)->second.max_value_ = limit;
  param_map.find(settings::Param::record_buffer_segment_size)->second.max_value_ = limit;
  param_map.find(settings::Param::record_buffer_segment_reuse)->second.max_value_ = limit;

  // Set Network Port
  param_map.find(settings::Param::port)->second.value_ = parser::ConstantValueExpression(
      type::TypeId::INTEGER, execution::sql::Integer(noisepage::runner::settings.port_));

  // Disable background metrics thread since we control metrics
  param_map.find(settings::Param::use_metrics_thread)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::BOOLEAN, sql_false);

  // Disable WAL overhead
  param_map.find(settings::Param::wal_enable)->second.value_ =
      parser::ConstantValueExpression(type::TypeId::BOOLEAN, sql_false);

  auto db_main_builder = DBMain::Builder()
                             .SetUseGC(true)
                             .SetUseCatalog(true)
                             .SetUseStatsStorage(true)
                             .SetUseMetrics(true)
                             .SetUseExecution(true)
                             .SetUseTrafficCop(true)
                             .SetUseNetwork(true)
                             .SetUseSettingsManager(true)
                             .SetSettingsParameterMap(std::move(param_map))
                             .SetNetworkPort(noisepage::runner::settings.port_);

  db_main = db_main_builder.Build().release();
  MiniRunnersExecUtil::DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(db_main, true);
  MiniRunnersExecUtil::DbMainSetParam<settings::Param::pipeline_metrics_interval, int>(db_main, 0);

  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();

  // Create the database
  auto txn = txn_manager->BeginTransaction();
  settings.db_oid_ = catalog->CreateDatabase(common::ManagedPointer(txn), "test_db", true);

  if (settings.load_upfront_) {
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings.db_oid_, DISABLED);
    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
        settings.db_oid_, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor),
        exec_settings, db_main->GetMetricsManager());

    execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
    table_gen.GenerateMiniRunnersData(settings, config);
  }

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();

  auto network_layer = noisepage::runner::db_main->GetNetworkLayer();
  auto server = network_layer->GetServer();
  server->RunServer();
}

void ShutdownRunners() {
  noisepage::execution::ExecutionUtil::ShutdownTPL();
  db_main->GetMetricsManager()->Aggregate();
  db_main->GetMetricsManager()->ToCSV();
  // free db main here so we don't need to use the loggers anymore
  delete db_main;
}

void CreateMiniRunnerTable(std::string table_name) {
  EXECUTION_LOG_INFO("Creating table {}", table_name);
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto block_store = db_main->GetStorageLayer()->GetBlockStore();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto txn = txn_manager->BeginTransaction();

  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings.db_oid_, DISABLED);
  auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(
      settings.db_oid_, common::ManagedPointer(txn), nullptr, nullptr, common::ManagedPointer(accessor), exec_settings,
      db_main->GetMetricsManager());

  execution::sql::TableGenerator table_gen(exec_ctx.get(), block_store, accessor->GetDefaultNamespace());
  table_gen.BuildMiniRunnerTable(table_name);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();
}

void DropMiniRunnerTable(std::string table_name) {
  EXECUTION_LOG_INFO("Dropping table {}", table_name);
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto txn = txn_manager->BeginTransaction();

  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings.db_oid_, DISABLED);
  auto tbl_oid = accessor->GetTableOid(table_name);
  accessor->DropTable(tbl_oid);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();
}

void ExecuteDescriptor(std::ofstream &target, MiniRunnerExecutorDescriptor &descriptor) {
  auto *executor = descriptor.GetExecutor();
  if (executor->RequiresExternalMetricsControl()) {
    db_main->GetMetricsManager()->RegisterThread();
  }

  // Run the iterations
  for (auto &arg : descriptor.GetArguments()) {
    executor->ExecuteIteration(arg, descriptor.GetMode());
    if (executor->RequiresGCCleanup()) {
      InvokeGC();
    }
  }

  if (executor->RequiresExternalMetricsControl()) {
    db_main->GetMetricsManager()->Aggregate();
    db_main->GetMetricsManager()->UnregisterThread();
  }

  // Write to correct output file
  std::vector<std::ofstream> files(1);
  files[0].swap(target);
  uint16_t pipeline_idx = static_cast<uint16_t>(metrics::MetricsComponent::EXECUTION_PIPELINE);
  db_main->GetMetricsManager()->AggregatedMetrics()[pipeline_idx]->ToCSV(&files);
  target.swap(files[0]);
}

void ExecuteRunners() {
  // Compute filters
  std::unordered_set<std::string> filters;
  if (settings.target_runner_specified_) {
    std::string token;
    std::istringstream stream(settings.target_filter_);
    while (std::getline(stream, token, ',')) {
      filters.insert(token);
    }
  }

  // Open all file streams
  std::ofstream streams[NUM_EXECUTORS];
  std::unordered_map<std::string, size_t> stream_map;
  std::unordered_map<std::string, std::string> insert_map;
  for (size_t i = 0; i < NUM_EXECUTORS; i++) {
    auto *executor = executors[i];
    if (insert_map.find(executor->GetFileName()) == insert_map.end()) {
      insert_map[executor->GetFileName()] = executor->GetName();

      streams[i].open(executor->GetFileName(), std::ofstream::trunc);
      stream_map[executor->GetName()] = i;
      streams[i] << metrics::PipelineMetricRawData::FEATURE_COLUMNS[0] << ", ";
      streams[i] << common::ResourceTracker::Metrics::COLUMNS << "\n";
    } else {
      // Share the existing stream
      stream_map[executor->GetName()] = stream_map[insert_map[executor->GetFileName()]];
    }
  }

  MiniRunnerScheduler scheduler;
  int rerun = settings.rerun_iterations_ + 1;
  for (int i = 0; i < rerun; i++) {
    // Generate arguments
    if (i == 0 || i == 1) {
      auto vm_modes = {noisepage::execution::vm::ExecutionMode::Interpret,
                       noisepage::execution::vm::ExecutionMode::Compiled};
      for (auto mode : vm_modes) {
        for (auto executor : executors) {
          executor->RegisterIterations(&scheduler, i != 0, mode);
        }
      }
    } else {
      scheduler.ClearSchedules();
    }

    size_t num_iterations = scheduler.NumIterations();
    size_t num_executed = 0;
    scheduler.Rewind();

    std::set<std::string> existing_tables;
    while (scheduler.HasNextSchedule()) {
      MiniRunnerSchedule schedule = scheduler.GetSchedule();
      {
        // Add needed tables
        const auto &schedule_tables = schedule.GetTables();
        for (auto &tbl : schedule_tables) {
          if (existing_tables.find(tbl) == existing_tables.end()) {
            CreateMiniRunnerTable(tbl);
          }
        }

        // Drop unneeded tables
        for (auto &tbl : existing_tables) {
          if (schedule_tables.find(tbl) == schedule_tables.end()) {
            DropMiniRunnerTable(tbl);
          }
        }

        // Resolve tables
        existing_tables = schedule_tables;
      }

      // Execute descriptors
      for (MiniRunnerExecutorDescriptor &descriptor : schedule.GetDescriptors()) {
        auto *executor = descriptor.GetExecutor();
        if (filters.empty() || filters.find(executor->GetName()) != filters.end()) {
          size_t index = stream_map[executor->GetName()];
          ExecuteDescriptor(streams[index], descriptor);
        }

        num_executed += descriptor.GetArguments().size();
        EXECUTION_LOG_INFO("[{}/{}] Data Point [{}/{}]\r", i, rerun, num_executed, num_iterations);
      }

      scheduler.AdvanceSchedule();
    }

    for (auto &tbl : existing_tables) {
      DropMiniRunnerTable(tbl);
    }
  }
}

};  // namespace noisepage::runner

int main(int argc, char **argv) {
  // Initialize mini-runner arguments
  noisepage::runner::settings.InitializeFromArguments(argc, argv);
  noisepage::runner::InitializeRunnersState();

  noisepage::runner::ExecuteRunners();

  noisepage::runner::ShutdownRunners();
  noisepage::LoggersUtil::ShutDown();
  return 0;
}
