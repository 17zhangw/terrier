#include <type_traits>

#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"

namespace noisepage::runner {

/**
 * Taken from Facebook's folly library.
 * DoNotOptimizeAway helps to ensure that a certain variable
 * is not optimized away by the compiler.
 */
template <typename T>
struct DoNotOptimizeAwayNeedsIndirect {
  using Decayed = typename std::decay<T>::type;

  // First two constraints ensure it can be an "r" operand.
  // std::is_pointer check is because callers seem to expect that
  // doNotOptimizeAway(&x) is equivalent to doNotOptimizeAway(x).
  constexpr static bool VALUE = !std::is_trivially_copyable<Decayed>::value ||
                                sizeof(Decayed) > sizeof(int64_t) || std::is_pointer<Decayed>::value;
};

template <typename T>
auto DoNotOptimizeAway(const T &datum) -> typename std::enable_if<!DoNotOptimizeAwayNeedsIndirect<T>::VALUE>::type {
  // The "r" constraint forces the compiler to make datum available
  // in a register to the asm block, which means that it must have
  // computed/loaded it.  We use this path for things that are <=
  // sizeof(long) (they have to fit), trivial (otherwise the compiler
  // doesn't want to put them in a register), and not a pointer (because
  // DoNotOptimizeAway(&foo) would otherwise be a foot gun that didn't
  // necessarily compute foo).
  //
  // An earlier version of this method had a more permissive input operand
  // constraint, but that caused unnecessary variation between clang and
  // gcc benchmarks.
  asm volatile("" ::"r"(datum));
}

/**
 * To prevent the iterator op i from being optimized out on
 * clang/gcc compilers, gcc requires noinline and noclone
 * to prevent any inlining. noclone is used to prevent gcc
 * from doing interprocedural constant propagation.
 *
 * DoNotOptimizeAway is placed inside the for () loop to
 * ensure that the compiler does not blindly optimize away
 * the for loop.
 *
 * TIGHT_LOOP_OPERATION(uint32_t, PLUS, +)
 * Defines a function called uint32_t_PLUS that adds integers.
 */
#ifdef __clang__
#define TIGHT_LOOP_OPERATION(type, name, op)                       \
  __attribute__((noinline)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                             \
    for (size_t i = 1; i <= count; i++) {                          \
      iterator = iterator op i;                                    \
      DoNotOptimizeAway(iterator);                                 \
    }                                                              \
                                                                   \
    return iterator;                                               \
  }

#elif __GNUC__
#define TIGHT_LOOP_OPERATION(type, name, op)                                \
  __attribute__((noinline, noclone)) type __##type##_##name(size_t count) { \
    type iterator = 1;                                                      \
    for (size_t i = 1; i <= count; i++) {                                   \
      iterator = iterator op i;                                             \
      DoNotOptimizeAway(iterator);                                          \
    }                                                                       \
                                                                            \
    return iterator;                                                        \
  }

#endif

TIGHT_LOOP_OPERATION(uint32_t, PLUS, +);
TIGHT_LOOP_OPERATION(uint32_t, MULTIPLY, *);
TIGHT_LOOP_OPERATION(uint32_t, DIVIDE, /);
TIGHT_LOOP_OPERATION(uint32_t, GEQ, >=);
TIGHT_LOOP_OPERATION(double, PLUS, +);
TIGHT_LOOP_OPERATION(double, MULTIPLY, *);
TIGHT_LOOP_OPERATION(double, DIVIDE, /);
TIGHT_LOOP_OPERATION(double, GEQ, >=);

void MiniRunnerArithmeticExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                      execution::vm::ExecutionMode mode) {
  if (rerun) {
    // Don't rerun this runner for reruns
    return;
  }

  if (mode != execution::vm::ExecutionMode::Interpret) {
    return;
  }

  MiniRunnerArguments arguments;
  auto operators = {selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE,
                    selfdriving::ExecutionOperatingUnitType::OP_INTEGER_COMPARE,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_PLUS_OR_MINUS,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_MULTIPLY,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_DIVIDE,
                    selfdriving::ExecutionOperatingUnitType::OP_REAL_COMPARE,
                    selfdriving::ExecutionOperatingUnitType::OP_VARCHAR_COMPARE};

  std::vector<size_t> counts;
  for (size_t i = 10000; i < 100000; i += 10000) counts.push_back(i);
  for (size_t i = 100000; i < 1000000; i += 100000) counts.push_back(i);

  for (auto op : operators) {
    for (auto count : counts) {
      std::vector<int64_t> args{static_cast<int64_t>(op), static_cast<int64_t>(count)};
      arguments.emplace_back(std::move(args));
    }
  }

  scheduler->CreateSchedule({}, this, mode, std::move(arguments));
}

void MiniRunnerArithmeticExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                    execution::vm::ExecutionMode mode) {
  auto type = static_cast<selfdriving::ExecutionOperatingUnitType>(iteration[0]);
  size_t num_elem = iteration[1];

  auto manager = (*db_main_)->GetTransactionLayer()->GetTransactionManager();
  auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
  auto qid = execution::query_id_t(0);
  auto db_oid = settings_->db_oid_;

  auto txn = manager->BeginTransaction();
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);

  auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), nullptr,
                                                                      nullptr, common::ManagedPointer(accessor),
                                                                      exec_settings, (*db_main_)->GetMetricsManager());
  exec_ctx->SetExecutionMode(static_cast<uint8_t>(execution::vm::ExecutionMode::Interpret));

  selfdriving::PipelineOperatingUnits units;
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer(&units));
  pipe0_vec.emplace_back(execution::translator_id_t(1), type, num_elem, 4, 1, num_elem, 1, 0, 0);
  units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  selfdriving::ExecOUFeatureVector ouvec;
  exec_ctx->InitializeOUFeatureVector(&ouvec, execution::pipeline_id_t(1));
  switch (type) {
    case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_PLUS_OR_MINUS: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      uint32_t ret = __uint32_t_PLUS(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_MULTIPLY: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      uint32_t ret = __uint32_t_MULTIPLY(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_DIVIDE: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      uint32_t ret = __uint32_t_DIVIDE(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_INTEGER_COMPARE: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      uint32_t ret = __uint32_t_GEQ(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_REAL_PLUS_OR_MINUS: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      double ret = __double_PLUS(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_REAL_MULTIPLY: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      double ret = __double_MULTIPLY(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_REAL_DIVIDE: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      double ret = __double_DIVIDE(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_REAL_COMPARE: {
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      double ret = __double_GEQ(num_elem);
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    case selfdriving::ExecutionOperatingUnitType::OP_VARCHAR_COMPARE: {
      uint32_t multiply_factor = sizeof(storage::VarlenEntry) / sizeof(uint32_t);
      auto *val = reinterpret_cast<storage::VarlenEntry *>(new uint32_t[num_elem * multiply_factor * 2]);
      for (size_t i = 0; i < num_elem * 2; ++i) {
        std::string str_val = std::to_string(i & 0xFF);
        val[i] = storage::VarlenEntry::Create(str_val);
      }
      uint32_t ret = 0;
      exec_ctx->StartPipelineTracker(execution::pipeline_id_t(1));
      for (size_t i = 0; i < num_elem; i++) {
        ret += storage::VarlenEntry::Compare(val[i], val[i + num_elem]);
        DoNotOptimizeAway(ret);
      }
      exec_ctx->EndPipelineTracker(qid, execution::pipeline_id_t(1), &ouvec);
      DoNotOptimizeAway(ret);
      break;
    }
    default:
      UNREACHABLE("Unsupported ExecutionOperatingUnitType");
      break;
  }

  ouvec.Reset();
  manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
}

};  // namespace noisepage::runner
