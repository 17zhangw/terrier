#include "loggers/runner_logger.h"
#include "spdlog/spdlog.h"

#include <memory>

namespace noisepage::runner {
#ifdef NOISEPAGE_USE_LOGGING
std::shared_ptr<spdlog::logger> runner_logger = nullptr;  // NOLINT

void InitRunnerLogger() {
  if (runner_logger == nullptr) {
    runner_logger = std::make_shared<spdlog::logger>("runner_logger", ::default_sink);  // NOLINT

    // Default format
    auto f = std::make_unique<spdlog::pattern_formatter>(spdlog::pattern_time_type::local, "");
    runner_logger->set_formatter(std::move(f));

    spdlog::register_logger(runner_logger);
  }
}
#endif
}  // namespace noisepage::runner
