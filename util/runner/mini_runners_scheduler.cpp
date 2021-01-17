#include "runner/mini_runners_scheduler.h"
#include <iostream>
#include <sstream>
#include "runner/mini_runners_executor.h"

namespace noisepage::runner {

std::ostream &operator<<(std::ostream &out, const MiniRunnerExecutorDescriptor &s) {
  out << s.executor_->GetName() << " [" << s.arguments_.size() << "]\n";
  return out;
}

std::ostream &operator<<(std::ostream &out, const MiniRunnerSchedule &s) {
  out << "Schedule:\n";
  for (auto &desc : s.descriptors_) {
    out << "\t-" << desc;
  }

  out << "\n\t- Table Dependencies: ";
  for (auto &tbl : s.tables_) {
    out << tbl << " ";
  }
  out << "\n\t- Index Dependencies: ";
  for (auto &idx : s.indexes_) {
    out << idx << " ";
  }
  out << "\n";
  return out;
}

void MiniRunnerScheduler::CreateSchedule(std::set<std::string> tables, std::set<std::string> indexes,
                                         MiniRunnerExecutor *executor, execution::vm::ExecutionMode mode,
                                         MiniRunnerArguments arguments) {
  MiniRunnerScheduleKey key(tables, indexes);
  MiniRunnerExecutorDescriptor desc(executor, mode, arguments);
  if (schedules_.find(key) == schedules_.end()) {
    schedules_.emplace(std::make_pair(key, MiniRunnerSchedule(tables, indexes)));
  }
  schedules_[key].AddDescriptor(std::move(desc));
  it_ = schedules_.begin();
}

void MiniRunnerScheduler::Rewind() {
  it_ = schedules_.begin();

  /*
  for (auto &it : schedules_) {
    std::cout << it.second << "\n";
  }
  */
}

bool MiniRunnerScheduler::HasNextSchedule() { return it_ != schedules_.end(); }

void MiniRunnerScheduler::AdvanceSchedule() { it_++; }

MiniRunnerSchedule &MiniRunnerScheduler::GetSchedule() { return it_->second; }

};  // namespace noisepage::runner
