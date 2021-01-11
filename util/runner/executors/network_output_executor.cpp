#include <pqxx/pqxx>
#include <type_traits>

#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"

namespace noisepage::runner {

void MiniRunnerNetworkOutputExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                         execution::vm::ExecutionMode mode) {
  if (rerun) {
    return;
  }

  if (mode == execution::vm::ExecutionMode::Interpret) {
    return;
  }

  scheduler->CreateSchedule({}, this, mode, {});
}

void MiniRunnerNetworkOutputExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                       execution::vm::ExecutionMode mode) {
  std::string conn;
  {
    std::stringstream conn_ss;
    conn_ss << "postgresql://127.0.0.1:" << (settings_->port_) << "/test_db";
    conn = conn_ss.str();
  }

  pqxx::connection c{conn};
  pqxx::work txn{c};
  {
    std::ostream null{nullptr};
    auto num_cols = {1, 3, 5, 7, 9, 11, 13, 15};
    auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
    std::vector<int64_t> row_nums = {1, 3, 5, 7, 10, 50, 100, 500, 1000, 2000, 5000, 10000};

    bool metrics_enabled = (*db_main_)->GetSettingsManager()->GetBool(settings::Param::pipeline_metrics_enable);
    MiniRunnersExecUtil::DbMainSetParam<settings::Param::counters_enable, bool>(*db_main_, false);
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          // Scale # iterations accordingly
          // Want to warmup the first query
          int iters = 1;
          if (row == 1 && col == 1 && type == type::TypeId::INTEGER) {
            iters += settings_->warmup_iterations_num_;
          }

          for (int i = 0; i < iters; i++) {
            if (i != iters - 1 && metrics_enabled) {
              MiniRunnersExecUtil::DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(*db_main_, false);
              metrics_enabled = false;
            } else if (i == iters - 1 && !metrics_enabled) {
              MiniRunnersExecUtil::DbMainSetParam<settings::Param::pipeline_metrics_enable, bool>(*db_main_, true);
              metrics_enabled = true;
            }

            std::stringstream query_ss;
            std::string type_s = (type == type::TypeId::INTEGER) ? "int" : "real";

            query_ss << "SELECT nprunnersemit" << type_s << "(" << row << "," << col << ",";
            if (type == type::TypeId::INTEGER)
              query_ss << col << ",0)";
            else
              query_ss << "0," << col << ")";

            if (col > 1) {
              query_ss << ",";
              for (int j = 1; j < col; j++) {
                query_ss << "nprunnersdummy" << type_s << "()";
                if (j != col - 1) {
                  query_ss << ",";
                }
              }
            }

            // Execute query
            pqxx::result r{txn.exec(query_ss.str())};

            // Get all the results
            for (const auto &result_row : r) {
              for (auto j = 0; j < col; j++) {
                null << result_row[j];
              }
            }
          }
        }
      }
    }
  }
  txn.commit();

  (*db_main_)->GetMetricsManager()->Aggregate();
  (*db_main_)->GetMetricsManager()->ToCSV();
}

};  // namespace noisepage::runner
