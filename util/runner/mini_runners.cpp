#include "common/resource_tracker.h"
#include "execution/execution_util.h"
#include "execution/table_generator/table_generator.h"
#include "main/db_main.h"
#include "runner/mini_runners_data_config.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
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

struct ExecutorDescriptor {
  explicit ExecutorDescriptor(MiniRunnerExecutor *executor, execution::vm::ExecutionMode mode,
                              MiniRunnerArguments &&arguments)
      : executor_(executor), mode_(mode), arguments_(arguments) {}

  MiniRunnerExecutor *executor_;
  execution::vm::ExecutionMode mode_;
  MiniRunnerArguments arguments_;
};

/**
 * Mini-Runner number of executors
 */
#define NUM_EXECUTORS (3)

/**
 * Mini-Runner executors
 */
MiniRunnerExecutor *executors[NUM_EXECUTORS] = {new MiniRunnerArithmeticExecutor(&config, &settings, &db_main),
                                                new MiniRunnerOutputExecutor(&config, &settings, &db_main),
                                                new MiniRunnerSeqScanExecutor(&config, &settings, &db_main)};

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
  auto catalog = db_main->GetCatalogLayer()->GetCatalog();
  auto txn_manager = db_main->GetTransactionLayer()->GetTransactionManager();
  auto txn = txn_manager->BeginTransaction();

  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings.db_oid_, DISABLED);
  auto tbl_oid = accessor->GetTableOid(table_name);
  accessor->DropTable(tbl_oid);

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  InvokeGC();
}

void ExecuteDescriptor(std::ofstream &target, const ExecutorDescriptor &descriptor) {
  auto *executor = descriptor.executor_;
  if (executor->RequiresExternalMetricsControl()) {
    db_main->GetMetricsManager()->RegisterThread();
  }

  // Run the iterations
  for (auto &arg : descriptor.arguments_) {
    executor->ExecuteIteration(arg, descriptor.mode_);
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

  if (executor->RequiresGCCleanup()) {
    InvokeGC();
  }
}

void ExecuteRunners() {
  std::unordered_set<std::string> filters;
  if (settings.target_runner_specified_) {
    std::string token;
    std::istringstream stream(settings.target_filter_);
    while (std::getline(stream, token, ',')) {
      filters.insert(token);
    }
  }

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

  auto vm_modes = {noisepage::execution::vm::ExecutionMode::Interpret,
                   noisepage::execution::vm::ExecutionMode::Compiled};

  int rerun = settings.rerun_iterations_ + 1;
  for (int i = 0; i < rerun; i++) {
    size_t num_iterations = 0;
    std::map<std::string, std::vector<ExecutorDescriptor>> descriptors;
    for (auto mode : vm_modes) {
      for (auto executor : executors) {
        auto data = executor->ConstructTableArgumentsMapping(i != 0, mode);
        for (auto record : data) {
          num_iterations += record.second.size();
          descriptors[record.first].emplace_back(executor, mode, std::move(record.second));
        }
      }
    }

    size_t num_executed = 0;
    for (auto iterator : descriptors) {
      std::string table_name = iterator.first;
      bool built_table = false;
      bool need_table = table_name != EmptyTableIdentifier;

      for (auto &descriptor : iterator.second) {
        auto *executor = descriptor.executor_;
        if (filters.empty() || filters.find(executor->GetName()) != filters.end()) {
          size_t index = stream_map[executor->GetName()];
          if (need_table) {
            // Build the table
            CreateMiniRunnerTable(table_name);

            built_table = true;
            need_table = false;
          }

          ExecuteDescriptor(streams[index], descriptor);
        }

        num_executed += descriptor.arguments_.size();
        EXECUTION_LOG_INFO("[{}/{}] Data Point [{}/{}]\r", i, rerun, num_executed, num_iterations);
      }

      if (built_table) {
        DropMiniRunnerTable(table_name);
      }
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
