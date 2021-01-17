#include "runner/mini_runners_settings.h"

#include <cstdlib>
#include <cstring>

#include "loggers/settings_logger.h"

namespace noisepage::runner {

struct Arg {
  const char *match_;
  bool found_;
  const char *value_;
  int int_value_;
};

void MiniRunnersSettings::InitializeFromArguments(int argc, char **argv) {
  Arg port_info{"--port=", false};

  Arg filter_info{"--filter=", false};
  Arg skip_large_rows_runs_info{"--skip_large_rows_runs=", false};
  Arg warm_num_info{"--warm_num=", false};
  Arg index_warm_num_info{"--index_model_warm_num=", false};
  Arg rerun_info{"--rerun=", false};
  Arg updel_info{"--updel_limit=", false};
  Arg warm_limit_info{"--warm_limit=", false};
  Arg create_index_small_data{"--create_index_small_limit=", false};
  Arg create_index_car_data{"--create_index_large_car_num=", false};
  Arg run_limit{"--mini_runner_rows_limit=", false};
  Arg batch_size{"--index_model_batch_size=", false};
  Arg load_upfront{"--load_upfront=", false};
  Arg log_detail{"--log_detail=", false};
  Arg *args[] = {&port_info,
                 &filter_info,
                 &skip_large_rows_runs_info,
                 &warm_num_info,
                 &rerun_info,
                 &updel_info,
                 &warm_limit_info,
                 &create_index_small_data,
                 &create_index_car_data,
                 &run_limit,
                 &batch_size,
                 &index_warm_num_info,
                 &load_upfront,
                 &log_detail};

  for (int i = 0; i < argc; i++) {
    for (auto *arg : args) {
      if (strstr(argv[i], arg->match_) != nullptr) {
        arg->found_ = true;
        arg->value_ = strstr(argv[i], "=") + 1;
        arg->int_value_ = atoi(arg->value_);
      }
    }
  }

  if (filter_info.found_) {
    target_runner_specified_ = true;
    target_filter_ = filter_info.value_;
  } else {
    target_filter_ = "";
  }

  if (port_info.found_) port_ = port_info.int_value_;
  if (skip_large_rows_runs_info.found_) skip_large_rows_runs_ = true;
  if (warm_num_info.found_) warmup_iterations_num_ = warm_num_info.int_value_;
  if (index_warm_num_info.found_) index_model_warmup_iterations_num_ = index_warm_num_info.int_value_;
  if (rerun_info.found_) rerun_iterations_ = rerun_info.int_value_;
  if (updel_info.found_) updel_limit_ = updel_info.int_value_;
  if (warm_limit_info.found_) warmup_rows_limit_ = warm_limit_info.int_value_;
  if (create_index_small_data.found_) create_index_small_limit_ = create_index_small_data.int_value_;
  if (create_index_car_data.found_) create_index_large_cardinality_num_ = create_index_car_data.int_value_;
  if (run_limit.found_) data_rows_limit_ = run_limit.int_value_;
  if (batch_size.found_) index_model_batch_size_ = batch_size.int_value_;
  if (load_upfront.found_) load_upfront_ = true;
  if (log_detail.found_) log_detail_ = true;

  noisepage::LoggersUtil::Initialize();
  SETTINGS_LOG_INFO("Starting mini-runners with this parameter set:");
  SETTINGS_LOG_INFO("Port ({}): {}", port_info.match_, port_);
  SETTINGS_LOG_INFO("Skip Large Rows ({}): {}", skip_large_rows_runs_info.match_, skip_large_rows_runs_);
  SETTINGS_LOG_INFO("Warmup Iterations ({}): {}", warm_num_info.match_, warmup_iterations_num_);
  SETTINGS_LOG_INFO("Index Model Warmup Iterations ({}): {}", index_warm_num_info.match_,
                    index_model_warmup_iterations_num_);
  SETTINGS_LOG_INFO("Rerun Iterations ({}): {}", rerun_info.match_, rerun_iterations_);
  SETTINGS_LOG_INFO("Index Model Batch Size ({}): {}", batch_size.match_, index_model_batch_size_);
  SETTINGS_LOG_INFO("Update/Delete Index Limit ({}): {}", updel_info.match_, updel_limit_);
  SETTINGS_LOG_INFO("Create Index Small Build Limit ({}): {}", create_index_small_data.match_,
                    create_index_small_limit_);
  SETTINGS_LOG_INFO("Create Index Large Cardinality Number Vary ({}): {}", create_index_car_data.match_,
                    create_index_large_cardinality_num_);
  SETTINGS_LOG_INFO("Warmup Rows Limit ({}): {}", warm_limit_info.match_, warmup_rows_limit_);
  SETTINGS_LOG_INFO("Mini Runner Rows Limit ({}): {}", run_limit.match_, data_rows_limit_);
  SETTINGS_LOG_INFO("Load Upfront ({}): {}", load_upfront.match_, load_upfront_);
  SETTINGS_LOG_INFO("Filter ({}): {}", filter_info.match_, target_filter_);
  SETTINGS_LOG_INFO("Log Detail ({}): {}", log_detail.match_, log_detail_);
}

};  // namespace noisepage::runner
