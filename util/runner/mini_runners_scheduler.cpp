#include "runner/mini_runners_scheduler.h"

namespace noisepage::runner {

void MiniRunnerScheduler::CreateSchedule(
    std::set<std::string> tables,
    MiniRunnerExecutor *executor,
    execution::vm::ExecutionMode mode,
    MiniRunnerArguments arguments) {
  MiniRunnerScheduleKey key(tables);
  MiniRunnerExecutorDescriptor desc(executor, mode, arguments);
  if (schedules_.find(key) == schedules_.end()) {
    schedules_.emplace(std::make_pair(key, MiniRunnerSchedule(tables)));
    //schedules_[key] = MiniRunnerSchedule(tables);
  }
  schedules_[key].AddDescriptor(std::move(desc));
  it_ = schedules_.begin();
}

void MiniRunnerScheduler::Rewind() {
  it_ = schedules_.begin();
}

bool MiniRunnerScheduler::HasNextSchedule() {
  return it_ != schedules_.end();
}

void MiniRunnerScheduler::AdvanceSchedule() {
  it_++;
}

MiniRunnerSchedule &MiniRunnerScheduler::GetSchedule() {
  return it_->second;
}

};  // namespace noisepage::runner
