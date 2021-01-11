#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "main/db_main.h"
#include "runner/mini_runners_data_config.h"
#include "runner/mini_runners_scheduler.h"
#include "runner/mini_runners_settings.h"

namespace noisepage::runner {

/**
 * A executor describes how to generate and model data for a single operating unit.
 */
class MiniRunnerExecutor {
 public:
  explicit MiniRunnerExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : config_(config), settings_(settings), db_main_(db_main) {}

  /**
   * Whether the executor requires the instrumenter to setup metrics support.
   */
  virtual bool RequiresExternalMetricsControl(void) = 0;

  /**
   * Whether GC should be performed after running an iteration
   */
  virtual bool RequiresGCCleanup(void) = 0;

  /**
   * Function should register any iterations that need to be executed with
   * the scheduler.
   *
   * @param scheduler Scheduler to register with
   * @param rerun Whether the run is a rerun or not
   * @param mode Execution mode to run in
   */
  virtual void RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun, execution::vm::ExecutionMode mode) = 0;

  /**
   * Execute a given iteration.
   */
  virtual void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode) = 0;

  /** Get the executor name */
  virtual std::string GetName() = 0;

  /** Get the filename to write to */
  virtual std::string GetFileName() = 0;

 protected:
  MiniRunnersDataConfig *config_ = nullptr;
  MiniRunnersSettings *settings_ = nullptr;
  DBMain **db_main_ = nullptr;
};

class MiniRunnerArithmeticExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerArithmeticExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return true; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Arithmetic"; }
  std::string GetFileName() { return "execution_SEQ0.csv"; }
};

class MiniRunnerOutputExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerOutputExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Output"; }
  std::string GetFileName() { return "execution_SEQ0.csv"; }
};

class MiniRunnerSeqScanExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerSeqScanExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Scan"; }
  std::string GetFileName() { return "execution_SEQ1_0.csv"; }
};

class MiniRunnerIndexScanExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerIndexScanExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "IndexScan"; }
  std::string GetFileName() { return "execution_SEQ1_1.csv"; }
};

class MiniRunnerSortExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerSortExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Sort"; }
  std::string GetFileName() { return "execution_SEQ2.csv"; }
};

class MiniRunnerAggKeyExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerAggKeyExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "AggKey"; }
  std::string GetFileName() { return "execution_SEQ4.csv"; }
};

class MiniRunnerAggKeylessExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerAggKeylessExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "AggKeyless"; }
  std::string GetFileName() { return "execution_SEQ4.csv"; }
};

class MiniRunnerInsertExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerInsertExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return true; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Insert"; }
  std::string GetFileName() { return "execution_SEQ5_0.csv"; }
};

class MiniRunnerUpdateExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerUpdateExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Update"; }
  std::string GetFileName() { return "execution_SEQ6.csv"; }
};

class MiniRunnerDeleteExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerDeleteExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Delete"; }
  std::string GetFileName() { return "execution_SEQ7_1.csv"; }
};

class MiniRunnerCreateIndexExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerCreateIndexExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return true; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "CreateIndex"; }
  std::string GetFileName() { return "execution_SEQ8.csv"; }
};

class MiniRunnerIndexOperationExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerIndexOperationExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings,
                                            DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIndexOperation(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode,
                             bool is_insert);
};

class MiniRunnerIndexInsertExecutor : public MiniRunnerIndexOperationExecutor {
 public:
  explicit MiniRunnerIndexInsertExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerIndexOperationExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return true; }
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "IndexInsert"; }
  std::string GetFileName() { return "execution_SEQ5_1.csv"; }
};

class MiniRunnerIndexDeleteExecutor : public MiniRunnerIndexOperationExecutor {
 public:
  explicit MiniRunnerIndexDeleteExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerIndexOperationExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return true; }
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "IndexDelete"; }
  std::string GetFileName() { return "execution_SEQ7_0.csv"; }
};

class MiniRunnerIndexJoinExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerIndexJoinExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings, DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  void IndexNLJoinChecker(catalog::db_oid_t db_oid, std::string build_tbl, size_t num_cols,
                          common::ManagedPointer<transaction::TransactionContext> txn, planner::AbstractPlanNode *plan);

  std::string GetName() { return "IndexJoin"; }
  std::string GetFileName() { return "execution_SEQ1_1.csv"; }
};

class MiniRunnerHashJoinNonSelfExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerHashJoinNonSelfExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings,
                                             DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  void JoinNonSelfChecker(catalog::db_oid_t db_oid, std::string build_tbl,
                          common::ManagedPointer<transaction::TransactionContext> txn, planner::AbstractPlanNode *plan);

  std::string GetName() { return "HashJoinNonSelf"; }
  std::string GetFileName() { return "execution_SEQ3.csv"; }
};

class MiniRunnerHashJoinSelfExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerHashJoinSelfExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings,
                                          DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "HashJoinSelf"; }
  std::string GetFileName() { return "execution_SEQ3.csv"; }
};

class MiniRunnerNetworkOutputExecutor : public MiniRunnerExecutor {
 public:
  explicit MiniRunnerNetworkOutputExecutor(MiniRunnersDataConfig *config, MiniRunnersSettings *settings,
                                           DBMain **db_main)
      : MiniRunnerExecutor(config, settings, db_main) {}

  bool RequiresExternalMetricsControl(void) { return false; }
  bool RequiresGCCleanup(void) { return false; }
  void RegisterIterations(MiniRunnerScheduler *scheduler, bool is_rerun, execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "NetworkOutput"; }
  std::string GetFileName() { return "execution_SEQ0.csv"; }
};

};  // namespace noisepage::runner
