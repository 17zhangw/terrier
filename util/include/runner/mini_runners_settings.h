#pragma once

#include <cstdint>
#include "catalog/catalog_defs.h"

namespace noisepage::runner {

/**
 * Class that holds mini-runner settings
 */
class MiniRunnersSettings {
 public:
  /**
   * DB_OID
   */
  catalog::db_oid_t db_oid_{0};

  /**
   * Port
   */
  uint16_t port_ = 15721;

  /**
   * Number of warmup iterations
   */
  int64_t warmup_iterations_num_ = 5;

  /**
   * Number of index warmup iterations
   */
  int64_t index_model_warmup_iterations_num_ = 2;

  /**
   * Number of rerun iterations
   */
  int64_t rerun_iterations_ = 10;

  /**
   * Batch size for modeling index inserts/deletes
   */
  int64_t index_model_batch_size_ = 7;

  /**
   * Limit on the max number of rows / tuples processed
   */
  int64_t data_rows_limit_ = 1000000;

  /**
   * Limit on num_rows for which queries need warming up
   */
  int64_t warmup_rows_limit_ = 1000;

  /**
   * warmup_rows_limit controls which queries need warming up.
   * skip_large_rows_runs is used for controlling whether or not
   * to run queries with large rows (> warmup_rows_limit)
   */
  bool skip_large_rows_runs_ = false;

  /**
   * Update/Delete Index Scan Limit
   */
  int64_t updel_limit_ = 1000;

  /**
   * CREATE INDEX small build limit
   */
  int64_t create_index_small_limit_ = 10000;

  /**
   * Number of cardinalities to vary for CREATE INDEX large builds.
   */
  int64_t create_index_large_cardinality_num_ = 3;

  /**
   * Load upfront. Loading the data upfront means the runners will
   * be run in some serial interleaving (i.e. all scans, then all
   * sorts) as opposed to individual iterations being interleaved.
   */
  bool load_upfront_ = false;

  /**
   * Whether to run a targeted filter
   */
  bool target_runner_specified_ = false;

  /**
   * Log more extensively
   */
  bool log_detail_ = false;

  /**
   * Target filter
   */
  const char *target_filter_ = nullptr;

  /**
   * Initialize all the above settings from the arguments
   * @param argc Number of arguments
   * @param argv Arguments
   */
  void InitializeFromArguments(int argc, char **argv);
};

};  // namespace noisepage::runner
