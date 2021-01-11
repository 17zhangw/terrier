#include <type_traits>

#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"

namespace noisepage::runner {

void MiniRunnerOutputExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun, execution::vm::ExecutionMode mode) {
  MiniRunnerArguments arguments;
  auto &num_cols = config_->sweep_col_nums_;
  auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
  for (auto type : types) {
    for (int64_t col : num_cols) {
      for (int64_t row : row_nums) {
        int64_t int_cols = 0;
        int64_t real_cols = 0;
        if (type == type::TypeId::INTEGER)
          int_cols = col;
        else if (type == type::TypeId::REAL)
          real_cols = col;

        std::vector<int64_t> args({int_cols, real_cols, row});
        arguments.emplace_back(std::move(args));
      }
    }
  }

  // Generate special Output feature [1 0 0 1 1]
  std::vector<int64_t> args({0, 0, 1});
  arguments.emplace_back(std::move(args));
  scheduler->CreateSchedule({}, this, mode, std::move(arguments));
}

void MiniRunnerOutputExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                execution::vm::ExecutionMode mode) {
  auto num_integers = iteration[0];
  auto num_reals = iteration[1];
  auto row_num = iteration[2];
  auto num_col = num_integers + num_reals;

  std::stringstream output;
  output << "struct OutputStruct {\n";
  for (auto i = 0; i < num_integers; i++) output << "col" << i << " : Integer\n";
  for (auto i = num_integers; i < num_col; i++) output << "col" << i << " : Real\n";
  output << "}\n";

  output << "struct QueryState {\nexecCtx: *ExecutionContext\n}\n";
  output << "struct P1_State {\noutput_buffer: *OutputBuffer\nexecFeatures: ExecOUFeatureVector\n}\n";
  output << "fun Query0_Init(queryState: *QueryState) -> nil {\nreturn}\n";
  output << "fun Query0_Pipeline1_InitPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  output << "\tpipelineState.output_buffer = @resultBufferNew(queryState.execCtx)\n";
  output << "\treturn\n";
  output << "}\n";
  output << "fun Query0_Pipeline1_TearDownPipelineState(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  output << "\t@resultBufferFree(pipelineState.output_buffer)\n";
  output << "\t@execOUFeatureVectorReset(&pipelineState.execFeatures)\n";
  output << "}\n";

  // pipeline
  output << "fun Query0_Pipeline1_SerialWork(queryState: *QueryState, pipelineState: *P1_State) -> nil {\n";
  if (num_col > 0) {
    output << "\tvar out: *OutputStruct\n";
    output << "\tfor(var it = 0; it < " << row_num << "; it = it + 1) {\n";
    output << "\t\tout = @ptrCast(*OutputStruct, @resultBufferAllocRow(pipelineState.output_buffer))\n";
    output << "\t}\n";
  }
  output << "}\n";

  output << "fun Query0_Pipeline1_Init(queryState: *QueryState) -> nil {\n";
  output << "\tvar threadStateContainer = @execCtxGetTLS(queryState.execCtx)\n";
  output << "\t@tlsReset(threadStateContainer, @sizeOf(P1_State), Query0_Pipeline1_InitPipelineState, "
            "Query0_Pipeline1_TearDownPipelineState, queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_Run(queryState: *QueryState) -> nil {\n";
  output
      << "\tvar pipelineState = @ptrCast(*P1_State, @tlsGetCurrentThreadState(@execCtxGetTLS(queryState.execCtx)))\n";
  output << "\t@execOUFeatureVectorInit(queryState.execCtx, &pipelineState.execFeatures, 1, false)\n";
  output << "\t@execCtxStartPipelineTracker(queryState.execCtx, 1)\n";
  output << "\tQuery0_Pipeline1_SerialWork(queryState, pipelineState)\n";
  output << "\t@resultBufferFinalize(pipelineState.output_buffer)\n";
  output << "\t@execCtxEndPipelineTracker(queryState.execCtx, 0, 1, &pipelineState.execFeatures)\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_Pipeline1_TearDown(queryState: *QueryState) -> nil {\n";
  output << "\t@tlsClear(@execCtxGetTLS(queryState.execCtx))\n";
  output << "\treturn\n";
  output << "}\n";

  output << "fun Query0_TearDown(queryState: *QueryState) -> nil {\nreturn\n}\n";

  output << "fun main(queryState: *QueryState) -> nil {\n";
  output << "\tQuery0_Pipeline1_Init(queryState)\n";
  output << "\tQuery0_Pipeline1_Run(queryState)\n";
  output << "\tQuery0_Pipeline1_TearDown(queryState)\n";
  output << "\treturn\n";
  output << "}\n";

  std::vector<planner::OutputSchema::Column> cols;
  for (auto i = 0; i < num_integers; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::INTEGER, nullptr);
  }

  for (auto i = 0; i < num_reals; i++) {
    std::stringstream col;
    col << "col" << i;
    cols.emplace_back(col.str(), type::TypeId::REAL, nullptr);
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_integers + real_size * num_reals;

  auto txn_manager = (*db_main_)->GetTransactionLayer()->GetTransactionManager();
  auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
  auto db_oid = settings_->db_oid_;

  auto txn = txn_manager->BeginTransaction();
  auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
  auto schema = std::make_unique<planner::OutputSchema>(std::move(cols));

  auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
  execution::exec::NoOpResultConsumer consumer;
  execution::exec::OutputCallback callback = consumer;
  auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), callback,
                                                                      schema.get(), common::ManagedPointer(accessor),
                                                                      exec_settings, (*db_main_)->GetMetricsManager());

  auto exec_query =
      execution::compiler::ExecutableQuery(output.str(), common::ManagedPointer(exec_ctx), false, 16, exec_settings);

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::OUTPUT, row_num,
                         tuple_size, num_col, 0, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));
  exec_query.SetPipelineOperatingUnits(std::move(units));

  txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);

  {
    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, db_oid,        &exec_query, schema.get(), settings_->warmup_iterations_num_ + 1, true,
        mode,      exec_settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }
}

};  // namespace noisepage::runner
