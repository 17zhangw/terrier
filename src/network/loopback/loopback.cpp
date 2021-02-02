#include <sstream>

#include "loggers/network_logger.h"
#include "network/loopback/loopback.h"

namespace noisepage::network {

bool LoopbackConnection::Connect(const std::string &database) {
  std::string conn;
  {
    std::stringstream sstream;
    sstream << "postgresql://127.0.0.1:" << port_ << "/" << database;
    conn = sstream.str();
  }

  try {
    connection_ = new pqxx::connection(conn);
    return true;
  } catch (std::exception &e) {
    NETWORK_LOG_INFO("LoopbackConnection::Connect on port {} rejected {}", port_, e.what());
    return false;
  }
}

bool LoopbackConnection::ExecuteDDLs(const std::vector<std::string> &ddls) {
  pqxx::work txn{*connection_};
  try {
    for (auto &ddl : ddls) {
      txn.exec(ddl);
    }

    txn.commit();
    return true;
  } catch (std::exception &e) {
    txn.abort();
    NETWORK_LOG_INFO("LoopbackConnection::ExecuteDDLs failed {}", e.what());
    return false;
  }
}

template <class Accum>
bool LoopbackConnection::ExecuteDML(const std::string &dml, Accum *result, RowTransform<Accum> trans) {
  pqxx::work txn{*connection_};
  try {
    auto result = txn.exec(dml);
    for (const auto &row : result) {
      trans(result, row);
    }

    txn.commit();
    return true;
  } catch (std::exception &e) {
    txn.abort();
    NETWORK_LOG_INFO("LoopbackConnection::ExecuteDML failed {}", e.what());
    return false;
  }
}

LoopbackConnection::~LoopbackConnection() {
  if (connection_ != NULL) {
    try {
      connection_->disconnect();
    } catch (std::exception &e) {
      NETWORK_LOG_INFO("~LoopbackConnection::LoopbackConnection failed {}", e.what());
    }

    delete connection_;
    connection_ = NULL;
  }
}

};  // namespace noisepage::network
