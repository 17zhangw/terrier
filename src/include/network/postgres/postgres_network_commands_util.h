#pragma once
#include "network/network_command.h"

namespace noisepage::network {

class Portal;

class PostgresNetworkCommandsUtil {
 public:
  static void ExecutePortal(const common::ManagedPointer<network::ConnectionContext> connection_ctx,
                            const common::ManagedPointer<Portal> portal,
                            const common::ManagedPointer<network::PostgresPacketWriter> out,
                            const common::ManagedPointer<trafficcop::TrafficCop> t_cop, const bool explicit_txn_block);
};

};  // namespace noisepage::network
