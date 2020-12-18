#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "main/db_main.h"
#include "runner/mini_runners_data_config.h"
#include "runner/mini_runners_settings.h"

namespace noisepage::runner {

/**
 * Identifier to use for empty table identifier
 */
constexpr const char *EmptyTableIdentifier = "";

/**
 * Represents an argument for a single runner iteration. This is kept
 * as a parameterized vector since current runners do not require any
 * additional information.
 */
struct MiniRunnerIterationArgument {
  std::vector<int64_t> state;
};

using MiniRunnerArguments = std::vector<MiniRunnerIterationArgument>;
using TableArgumentMapping = std::map<std::string, MiniRunnerArguments>;

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
   * Function returns a mapping from table names to arguments for the
   * iterations to execute against that table. The table names should
   * be generated with TableGenerator::GenerateTableName().
   *
   * This function is used by the coordinator to understand what tables
   * this particular runner requires and what iterations need to be
   * executed against the table.
   *
   * If a runner does not require tables, this function should still
   * generate the iteration arguments. The EmptyTableIdentifier should
   * be used as the table name in that case.
   *
   * @param is_rerun Whether the run is a rerun or not
   * @param mode Execution mode to run in
   */
  virtual std::map<std::string, MiniRunnerArguments> ConstructTableArgumentsMapping(
      bool is_rerun, execution::vm::ExecutionMode mode) = 0;

  /**
   * Execute a given iteration.
   */
  virtual void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode) = 0;

  virtual std::string GetName() = 0;
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
  std::map<std::string, MiniRunnerArguments> ConstructTableArgumentsMapping(bool is_rerun,
                                                                            execution::vm::ExecutionMode mode);
  void ExecuteIteration(const MiniRunnerIterationArgument &iteration, execution::vm::ExecutionMode mode);

  std::string GetName() { return "Arithmetic"; }
  std::string GetFileName() { return "execution_SEQ0.csv"; }
};

};  // namespace noisepage::runner
