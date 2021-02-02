#pragma once

#include <pqxx/pqxx>

namespace noisepage::network {

/**
 * LoopbackConnection is used in cases where code within NoisePage wants to
 * execute SQL against the system itself. At a trade-off of performance,
 * this class provides a standard method for execution as opposed to
 * specially crafing calls to TrafficCop.
 */
class LoopbackConnection {
 public:
  /**
   * Function for a row transformation function.
   * This is invoked per result row in ExecuteDML.
   */
  template <class Accum>
  using RowTransform = void (*)(Accum *, const pqxx::row &row);

  /**
   * Constructor
   * @param port Port to connect to
   */
  explicit LoopbackConnection(int port)
    : port_(port) {}

  /**
   * Connect
   * @param database Database to connect to
   */
  bool Connect(const std::string &database);

  /**
   * Execute a DDL statement
   * @param ddls DDL statements to execute under single transaction
   * @return indicating whether statement succeeds
   */
  bool ExecuteDDLs(const std::vector<std::string> &ddls);

  /**
   * Execute a DML statement
   * @param dml DML statement to execute
   * @param result Result set for results to be placed into
   * @param trans Row transformation function
   * @return indicating whether DML succeeded or not
   */
  template <class Accum>
  bool ExecuteDML(const std::string &dml, Accum *result, RowTransform<Accum> trans);

  /**
   * Destructor
   */
  ~LoopbackConnection();

 private:
  int port_;
  pqxx::connection *connection_ = NULL;
};

}  // namespace noisepage::network
