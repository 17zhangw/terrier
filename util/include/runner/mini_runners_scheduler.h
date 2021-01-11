#pragma once

#include <set>
#include <map>
#include <vector>

#include "execution/vm/vm_defs.h"

namespace noisepage::runner {

class MiniRunnerExecutor;

/**
 * Represents an argument for a single runner iteration. This is kept
 * as a parameterized vector since current runners do not require any
 * additional information.
 */
using MiniRunnerIterationArgument = std::vector<int64_t>;
using MiniRunnerArguments = std::vector<MiniRunnerIterationArgument>;

/**
 * An executor descriptor comprises the target executor, the mode, and
 * the iteration arguments. This executor descriptor contains the
 * information to gather mini-runner data for a single operator.
 */
class MiniRunnerExecutorDescriptor {
 public:
  explicit MiniRunnerExecutorDescriptor(MiniRunnerExecutor *executor,
      execution::vm::ExecutionMode mode,
      MiniRunnerArguments arguments)
    : executor_(executor),
      mode_(mode),
      arguments_(arguments) {}

  MiniRunnerExecutor *GetExecutor() const { return executor_; }
  execution::vm::ExecutionMode GetMode() { return mode_; }
  MiniRunnerArguments &GetArguments() { return arguments_; }

 private:
  MiniRunnerExecutor *executor_;
  execution::vm::ExecutionMode mode_;
  MiniRunnerArguments arguments_;
};

/**
 * A key for ordering schedules. A schedule key is described solely by the
 * tables that are required to execute the schedule. This key performs
 * lexigraphical-ordering on the table sets.
 */
class MiniRunnerScheduleKey {
 public:
  MiniRunnerScheduleKey(std::set<std::string> tables)
    : tables_(tables) {}

  bool operator<(const MiniRunnerScheduleKey &s) const {
    // Lexicographically compare
    return tables_ < s.tables_;
  }

  bool operator==(const MiniRunnerScheduleKey &s) const {
    return tables_ == s.tables_;
  }

 private:
  /** Tables required */
  std::set<std::string> tables_;
};

/**
 * A schedule comprises a set of tables and the executors with iterations
 * to execute on that set of tables.
 */
class MiniRunnerSchedule {
 public:
  explicit MiniRunnerSchedule() {}
    
  explicit MiniRunnerSchedule(std::set<std::string> tables)
    : tables_(tables) {}

  void AddDescriptor(MiniRunnerExecutorDescriptor &&descriptor) {
    descriptors_.emplace_back(descriptor);
  }

  size_t NumIterations() {
    size_t num_iterations = 0;
    for (auto &descriptor : descriptors_) {
      num_iterations += descriptor.GetArguments().size();
    }
    return num_iterations;
  }

  const std::set<std::string> &GetTables() { return tables_; }
  std::vector<MiniRunnerExecutorDescriptor> &GetDescriptors() { return descriptors_; }

 private:
  /** Tables required to execute the descriptors */
  std::set<std::string> tables_;

  /** MiniRunner executors and arguments to execute */
  std::vector<MiniRunnerExecutorDescriptor> descriptors_;
};

class MiniRunnerScheduler {
 public:
  explicit MiniRunnerScheduler() {}

  /**
   * Create a schedule.
   *
   * @param tables Tables that are required
   * @param executor Executor
   * @param mode Execution mode
   * @param arguments Executor's iteration arguments
   */
  void CreateSchedule(std::set<std::string> tables,
      MiniRunnerExecutor *executor,
      execution::vm::ExecutionMode mode,
      MiniRunnerArguments arguments);

  size_t NumSchedules() {
    return schedules_.size();
  }

  size_t NumIterations() {
    size_t num_iterations = 0;
    for (auto it : schedules_) {
      num_iterations += it.second.NumIterations();
    }
    return num_iterations;
  }

  /** Wipe schedules */
  void ClearSchedules() {
    schedules_.clear();
  }

  /** Rewind the iterator to first schedule */
  void Rewind();

  /** Whether there is another schedule available */
  bool HasNextSchedule();

  /** Get the schedule */
  MiniRunnerSchedule &GetSchedule();

  /** Advance schedule */
  void AdvanceSchedule();

 private:
  /** List of schedules */
  std::map<MiniRunnerScheduleKey, MiniRunnerSchedule> schedules_;

  /** Iterator */
  std::map<MiniRunnerScheduleKey, MiniRunnerSchedule>::iterator it_;
};

};  // namespace noisepage::runner
