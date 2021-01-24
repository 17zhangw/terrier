#include "network/postgres/postgres_network_commands_util.h"

#include "common/thread_context.h"
#include "metrics/metrics_store.h"
#include "network/network_util.h"
#include "network/postgres/postgres_packet_util.h"
#include "network/postgres/postgres_protocol_interpreter.h"
#include "network/postgres/statement.h"
#include "traffic_cop/traffic_cop.h"

namespace noisepage::network {

void PostgresNetworkCommandsUtil::ExecutePortal(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                                                const common::ManagedPointer<Portal> portal,
                                                const common::ManagedPointer<network::PostgresPacketWriter> out,
                                                const common::ManagedPointer<trafficcop::TrafficCop> t_cop,
                                                const bool explicit_txn_block) {
  trafficcop::TrafficCopResult result;

  const auto query_type = portal->GetStatement()->GetQueryType();
  const auto physical_plan = portal->OptimizeResult()->GetPlanNode();

  // This logic relies on ordering of values in the enum's definition and is documented there as well.
  if (NetworkUtil::DMLQueryType(query_type)) {
    // DML query to put through codegen
    result = t_cop->CodegenPhysicalPlan(connection_ctx, out, portal);

    // TODO(Matt): do something with result here in case codegen fails

    result = t_cop->RunExecutableQuery(connection_ctx, out, portal);
  } else if (NetworkUtil::CreateQueryType(query_type)) {
    if (explicit_txn_block && query_type == network::QueryType::QUERY_CREATE_DB) {
      out->WriteError({common::ErrorSeverity::ERROR, "CREATE DATABASE cannot run inside a transaction block",
                       common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    if (query_type == network::QueryType::QUERY_CREATE_INDEX) {
      result = t_cop->ExecuteCreateStatement(connection_ctx, physical_plan, query_type);
      result = t_cop->CodegenPhysicalPlan(connection_ctx, out, portal);
      result = t_cop->RunExecutableQuery(connection_ctx, out, portal);
    } else {
      result = t_cop->ExecuteCreateStatement(connection_ctx, physical_plan, query_type);
    }
  } else if (NetworkUtil::DropQueryType(query_type)) {
    if (explicit_txn_block && query_type == network::QueryType::QUERY_DROP_DB) {
      out->WriteError({common::ErrorSeverity::ERROR, "DROP DATABASE cannot run inside a transaction block",
                       common::ErrorCode::ERRCODE_ACTIVE_SQL_TRANSACTION});
      connection_ctx->Transaction()->SetMustAbort();
      return;
    }
    result = t_cop->ExecuteDropStatement(connection_ctx, physical_plan, query_type);
  }

  if (result.type_ == trafficcop::ResultType::COMPLETE) {
    NOISEPAGE_ASSERT(std::holds_alternative<uint32_t>(result.extra_), "We're expecting number of rows here.");
    out->WriteCommandComplete(query_type, std::get<uint32_t>(result.extra_));
  } else {
    NOISEPAGE_ASSERT(result.type_ == trafficcop::ResultType::ERROR,
                     "Currently only expecting COMPLETE or ERROR from TrafficCop here.");
    NOISEPAGE_ASSERT(std::holds_alternative<common::ErrorData>(result.extra_), "We're expecting a message here.");
    out->WriteError(std::get<common::ErrorData>(result.extra_));
  }
}

};  // namespace noisepage::network
