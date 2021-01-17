#pragma once

#include <memory>

#include "loggers/loggers_util.h"

#ifdef NOISEPAGE_USE_LOGGING

namespace noisepage::runner {
extern std::shared_ptr<spdlog::logger> runner_logger;  // NOLINT

void InitRunnerLogger();
}  // namespace noisepage::runner

#define RUNNER_LOG_TRACE(...) ::noisepage::runner::runner_logger->trace(__VA_ARGS__)
#define RUNNER_LOG_DEBUG(...) ::noisepage::runner::runner_logger->debug(__VA_ARGS__)
#define RUNNER_LOG_INFO(...) ::noisepage::runner::runner_logger->info(__VA_ARGS__)
#define RUNNER_LOG_WARN(...) ::noisepage::runner::runner_logger->warn(__VA_ARGS__)
#define RUNNER_LOG_ERROR(...) ::noisepage::runner::runner_logger->error(__VA_ARGS__)

#else

#define RUNNER_LOG_TRACE(...) ((void)0)
#define RUNNER_LOG_DEBUG(...) ((void)0)
#define RUNNER_LOG_INFO(...) ((void)0)
#define RUNNER_LOG_WARN(...) ((void)0)
#define RUNNER_LOG_ERROR(...) ((void)0)

#endif
