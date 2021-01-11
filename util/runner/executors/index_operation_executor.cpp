#include <type_traits>

#include "execution/vm/bytecode_handlers.h"
#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"
#include "storage/sql_table.h"

namespace noisepage::runner {

void MiniRunnerIndexOperationExecutor::RegisterIterations(MiniRunnerScheduler *scheduler, bool rerun,
                                                          execution::vm::ExecutionMode mode) {
  if (mode == execution::vm::ExecutionMode::Compiled) {
    return;
  }

  std::map<std::string, MiniRunnerArguments> mapping;
  auto num_indexes = {settings_->index_model_batch_size_};
  const auto row_nums = config_->GetRowNumbersWithLimit(settings_->data_rows_limit_);
  auto types = {type::TypeId::INTEGER, type::TypeId::BIGINT};
  auto num_cols = config_->sweep_index_col_nums_;
  for (auto num_index : num_indexes) {
    for (auto type : types) {
      for (auto col : num_cols) {
        for (auto row : row_nums) {
          if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
            continue;
          }

          auto tbl = execution::sql::TableGenerator::GenerateTableName({type}, {15}, row, row);
          std::vector<int64_t> args{col, 15, row, static_cast<int64_t>(type), num_index};
          mapping[tbl].emplace_back(std::move(args));
        }
      }
    }
  }

  for (auto &map : mapping) {
    scheduler->CreateSchedule({map.first}, this, mode, std::move(map.second));
  }
}

void MiniRunnerIndexOperationExecutor::ExecuteIndexOperation(const MiniRunnerIterationArgument &iteration,
                                                             execution::vm::ExecutionMode mode, bool is_insert) {
  auto key_num = iteration[0];
  uint64_t tbl_cols = iteration[1];
  auto num_rows = iteration[2];
  auto type = static_cast<type::TypeId>(iteration[3]);
  auto num_index = iteration[4];
  auto target = num_rows + 1;

  // Create the indexes for batch-insert
  auto cols = MiniRunnersSqlUtil::ConstructSQLClause(type, type::TypeId::INVALID, key_num, 0, ", ", "", false, "");
  auto tbl_name = MiniRunnersSqlUtil::ConstructTableName(type, type::TypeId::INVALID, tbl_cols, 0, num_rows, num_rows);
  for (auto i = 0; i < num_index; i++) {
    auto settings = MiniRunnersExecUtil::GetExecutionSettings(false);
    auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();

    std::stringstream query;
    query << "CREATE INDEX idx" << i << " ON " << tbl_name << " (" << cols << ")";
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = query.str();
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = settings;
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    MiniRunnersExecUtil::ExecuteRequest req{
        *db_main_, settings_->db_oid_, equery.first.get(), equery.second.get(), 1, true, mode, settings, {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }

  // Invoke GC to clean some data
  auto gc = (*db_main_)->GetStorageLayer()->GetGarbageCollector();
  gc->PerformGarbageCollection();
  gc->PerformGarbageCollection();

  auto db_oid = settings_->db_oid_;
  auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
  auto txn_manager = (*db_main_)->GetTransactionLayer()->GetTransactionManager();
  int64_t num_iters = 1 + settings_->index_model_warmup_iterations_num_;
  for (int64_t iter = 0; iter < num_iters; iter++) {
    common::ManagedPointer<metrics::MetricsManager> metrics_manager = nullptr;
    if (iter == num_iters - 1) {
      metrics_manager = (*db_main_)->GetMetricsManager();
      metrics_manager->RegisterThread();
    }

    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), db_oid, DISABLED);
    auto tbl_oid = accessor->GetTableOid(tbl_name);
    auto idx_oids = accessor->GetIndexOids(tbl_oid);

    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    execution::exec::NoOpResultConsumer consumer;
    execution::exec::OutputCallback callback = consumer;
    auto exec_ctx = std::make_unique<execution::exec::ExecutionContext>(db_oid, common::ManagedPointer(txn), callback,
                                                                        nullptr, common::ManagedPointer(accessor),
                                                                        exec_settings, metrics_manager);

    // A brief discussion of the features:
    // NUM_ROWS: size of the index
    // KEY_SIZE: size of the keys
    // KEY_NUM: number of keys
    // Cardinality field: number of indexes being inserted into (i.e batch size)
    selfdriving::ExecOUFeatureVector features;
    selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
    auto feature_type = is_insert ? selfdriving::ExecutionOperatingUnitType::INDEX_INSERT
                                  : selfdriving::ExecutionOperatingUnitType::INDEX_DELETE;
    auto type_size = type::TypeUtil::GetTypeSize(type);
    auto key_size = type_size * key_num;
    pipe0_vec.emplace_back(execution::translator_id_t(1), feature_type, num_rows, key_size, key_num, num_index, 1, 0,
                           0);
    selfdriving::PipelineOperatingUnits units;
    units.RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));
    exec_ctx->SetPipelineOperatingUnits(common::ManagedPointer<selfdriving::PipelineOperatingUnits>(&units));
    exec_ctx->InitializeOUFeatureVector(&features, execution::pipeline_id_t(1));

    // Initialize Storage Interface
    uint32_t col_oids[tbl_cols];
    for (uint64_t i = 0; i < tbl_cols; i++) {
      col_oids[i] = i + 1;
    }

    // No columns if deleting
    execution::sql::StorageInterface si(exec_ctx.get(), tbl_oid, col_oids, is_insert ? tbl_cols : 0, true);
    storage::ProjectedRow *tbl_pr = nullptr;
    storage::TupleSlot slot;
    if (is_insert) {
      OpStorageInterfaceGetTablePR(&tbl_pr, &si);
      for (uint64_t i = 0; i < tbl_cols; i++) {
        execution::sql::Integer value(target);
        if (type == type::TypeId::INTEGER) {
          OpPRSetInt(tbl_pr, i, &value);
        } else if (type == type::TypeId::BIGINT) {
          OpPRSetBigInt(tbl_pr, i, &value);
        }
      }

      OpStorageInterfaceTableInsert(&slot, &si);
    } else {
      bool has_more;
      uint32_t col_oids[] = {1};
      bool done = false;
      execution::sql::TableVectorIterator tvi(exec_ctx.get(), tbl_oid.UnderlyingValue(), col_oids, 1);
      OpTableVectorIteratorPerformInit(&tvi);
      OpTableVectorIteratorNext(&has_more, &tvi);
      while (has_more) {
        // We will delete the first tuple. We need to do this iteration to
        // ensure that the Tuple is visible to the current transaction.
        execution::sql::VectorProjectionIterator *vpi = nullptr;
        OpTableVectorIteratorGetVPI(&vpi, &tvi);

        bool vpi_next;
        OpVPIHasNext(&vpi_next, vpi);
        while (vpi_next) {
          done = true;
          OpVPIGetSlot(&slot, vpi);

          // Do a TableDelete
          bool result;
          OpStorageInterfaceTableDelete(&result, &si, &slot);
          if (!result) {
            OpAbortTxn(exec_ctx.get());
          }

          break;
        }

        if (done) {
          break;
        }

        // Advance TVI
        OpTableVectorIteratorNext(&has_more, &tvi);
      }

      if (!done) {
        throw "Expected tuple to be deleted";
      }
    }

    // Measure the core index operation
    OpExecutionContextStartPipelineTracker(exec_ctx.get(), execution::pipeline_id_t(1));
    for (auto idx : idx_oids) {
      storage::ProjectedRow *idx_pr;
      OpStorageInterfaceGetIndexPR(&idx_pr, &si, idx.UnderlyingValue());
      for (auto col = 0; col < key_num; col++) {
        if (is_insert) {
          execution::sql::Integer val(0);
          if (type == type::TypeId::INTEGER) {
            OpPRGetInt(&val, tbl_pr, col);
            OpPRSetInt(idx_pr, col, &val);
          } else if (type == type::TypeId::BIGINT) {
            OpPRGetBigInt(&val, tbl_pr, col);
            OpPRSetBigInt(idx_pr, col, &val);
          }
        } else {
          execution::sql::Integer value(target);
          if (type == type::TypeId::INTEGER) {
            OpPRSetInt(idx_pr, col, &value);
          } else if (type == type::TypeId::BIGINT) {
            OpPRSetBigInt(idx_pr, col, &value);
          }
        }
      }

      bool result = true;
      if (is_insert)
        OpStorageInterfaceIndexInsert(&result, &si);
      else
        OpStorageInterfaceIndexDelete(&si, &slot);

      if (!result) {
        OpAbortTxn(exec_ctx.get());
      }
    }
    OpExecutionContextEndPipelineTracker(exec_ctx.get(), execution::query_id_t(0), execution::pipeline_id_t(1),
                                         &features);

    // For inserts/deletes, abort the transaction.
    // If insert, don't want prior inserts to affect next insert.
    // If delete, need to make sure tuple is still visible
    txn_manager->Abort(txn);

    if (iter == num_iters - 1) {
      metrics_manager->Aggregate();
      metrics_manager->UnregisterThread();
    }
  }

  // Drop the indexes
  for (auto i = 0; i < num_index; i++) {
    std::string index_name;
    {
      std::stringstream index;
      index << "idx" << i;
      index_name = index.str();
    }
    MiniRunnersExecUtil::DropIndexByName((*db_main_), db_oid, index_name);
  }
}

void MiniRunnerIndexInsertExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                     execution::vm::ExecutionMode mode) {
  ExecuteIndexOperation(iteration, mode, true);
}

void MiniRunnerIndexDeleteExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                     execution::vm::ExecutionMode mode) {
  ExecuteIndexOperation(iteration, mode, false);
}

};  // namespace noisepage::runner
