#include <type_traits>

#include "optimizer/cost_model/trivial_cost_model.h"
#include "runner/mini_runners_exec_util.h"
#include "runner/mini_runners_executor.h"
#include "runner/mini_runners_sql_util.h"
#include "self_driving/modeling/operating_unit_recorder.h"
#include "storage/sql_table.h"

namespace noisepage::runner {

std::map<std::string, MiniRunnerArguments> MiniRunnerInsertExecutor::ConstructTableArgumentsMapping(
    bool rerun, execution::vm::ExecutionMode mode) {
  if (mode == execution::vm::ExecutionMode::Compiled || rerun) {
    return {};
  }

  MiniRunnerArguments arguments;
  auto types = {type::TypeId::INTEGER, type::TypeId::REAL};
  auto &num_rows = config_->sweep_insert_row_nums_;
  auto &num_cols = config_->sweep_col_nums_;
  for (auto type : types) {
    for (auto col : num_cols) {
      for (auto row : num_rows) {
        if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
          continue;
        }

        int64_t int_cols = (type == type::TypeId::INTEGER) ? col : 0;
        int64_t real_cols = (type == type::TypeId::REAL) ? col : 0;
        std::vector<int64_t> args({int_cols, real_cols, col, row});
        arguments.emplace_back(MiniRunnerIterationArgument{std::move(args)});
      }
    }
  }

  auto &mixed_dist = config_->sweep_insert_mixed_dist_;
  for (auto mixed : mixed_dist) {
    for (auto row : num_rows) {
      if (row > settings_->warmup_rows_limit_ && settings_->skip_large_rows_runs_) {
        continue;
      }

      std::vector<int64_t> args({mixed.first, mixed.second, mixed.first + mixed.second, row});
      arguments.emplace_back(MiniRunnerIterationArgument{std::move(args)});
    }
  }

  std::map<std::string, MiniRunnerArguments> mapping;
  mapping[EmptyTableIdentifier] = std::move(arguments);
  return mapping;
}

void MiniRunnerInsertExecutor::ExecuteIteration(const MiniRunnerIterationArgument &iteration,
                                                execution::vm::ExecutionMode mode) {
  auto catalog = (*db_main_)->GetCatalogLayer()->GetCatalog();
  auto txn_manager = (*db_main_)->GetTransactionLayer()->GetTransactionManager();
  auto num_ints = iteration.state[0];
  auto num_reals = iteration.state[1];
  auto num_cols = iteration.state[2];
  auto num_rows = iteration.state[3];

  // Create temporary table schema
  std::vector<catalog::Schema::Column> cols;
  std::vector<std::pair<type::TypeId, int64_t>> info = {{type::TypeId::INTEGER, num_ints},
                                                        {type::TypeId::REAL, num_reals}};
  int col_no = 1;
  for (auto &i : info) {
    for (auto j = 1; j <= i.second; j++) {
      std::stringstream col_name;
      col_name << "col" << col_no++;
      if (i.first == type::TypeId::INTEGER) {
        cols.emplace_back(
            col_name.str(), i.first, false,
            noisepage::parser::ConstantValueExpression(type::TypeId::INTEGER, execution::sql::Integer(0)));
      } else {
        cols.emplace_back(col_name.str(), i.first, false,
                          noisepage::parser::ConstantValueExpression(type::TypeId::REAL, execution::sql::Real(0.f)));
      }
    }
  }
  catalog::Schema tmp_schema(cols);

  // Create table
  catalog::table_oid_t tbl_oid;
  {
    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings_->db_oid_, DISABLED);
    tbl_oid = accessor->CreateTable(accessor->GetDefaultNamespace(), "tmp_table", tmp_schema);
    auto &schema = accessor->GetSchema(tbl_oid);
    auto *tmp_table = new storage::SqlTable((*db_main_)->GetStorageLayer()->GetBlockStore(), schema);
    accessor->SetTablePointer(tbl_oid, tmp_table);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }

  std::string tuple_row;
  {
    std::stringstream tuple;
    tuple << "(";
    for (uint32_t i = 1; i <= num_cols; i++) {
      tuple << i;
      if (i != num_cols) {
        tuple << ",";
      } else {
        tuple << ")";
      }
    }
    tuple_row = tuple.str();
  }

  // Hack to preallocate some memory
  std::string query;
  query.reserve(tuple_row.length() * num_rows + num_rows + 100);

  query += "INSERT INTO tmp_table VALUES ";
  for (uint32_t idx = 0; idx < num_rows; idx++) {
    query += tuple_row;
    if (idx != num_rows - 1) {
      query += ", ";
    }
  }

  auto int_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::INTEGER);
  auto real_size = type::TypeUtil::GetTypeTrueSize(type::TypeId::REAL);
  auto tuple_size = int_size * num_ints + real_size * num_reals;

  auto units = std::make_unique<selfdriving::PipelineOperatingUnits>();
  selfdriving::ExecutionOperatingUnitFeatureVector pipe0_vec;
  pipe0_vec.emplace_back(execution::translator_id_t(1), selfdriving::ExecutionOperatingUnitType::INSERT, num_rows,
                         tuple_size, num_cols, num_rows, 1, 0, 0);
  units->RecordOperatingUnit(execution::pipeline_id_t(1), std::move(pipe0_vec));

  {
    auto exec_settings = MiniRunnersExecUtil::GetExecutionSettings(true);
    MiniRunnersExecUtil::OptimizeRequest optimize;
    optimize.db_main = (*db_main_);
    optimize.db_oid = settings_->db_oid_;
    optimize.query = std::move(query);
    optimize.cost_model = std::make_unique<optimizer::TrivialCostModel>();
    optimize.pipeline_units = std::move(units);
    optimize.exec_settings = exec_settings;
    auto equery = MiniRunnersExecUtil::OptimizeSqlStatement(&optimize);

    int64_t num_iters = 1 + (num_rows <= settings_->warmup_rows_limit_ ? settings_->warmup_iterations_num_ : 0);
    MiniRunnersExecUtil::ExecuteRequest req{*db_main_,
                                            settings_->db_oid_,
                                            equery.first.get(),
                                            equery.second.get(),
                                            num_iters,
                                            true,
                                            mode,
                                            exec_settings,
                                            {}};
    MiniRunnersExecUtil::ExecuteQuery(&req);
  }

  // Drop the table
  {
    auto txn = txn_manager->BeginTransaction();
    auto accessor = catalog->GetAccessor(common::ManagedPointer(txn), settings_->db_oid_, DISABLED);
    accessor->DropTable(tbl_oid);
    txn_manager->Commit(txn, transaction::TransactionUtil::EmptyCallback, nullptr);
  }
}

};  // namespace noisepage::runner
