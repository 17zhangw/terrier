#pragma once

#include "execution/compiler/operator/operator_translator.h"

namespace noisepage::planner {
class CSVScanPlanNode;
}  // namespace noisepage::planner

namespace noisepage::execution::compiler {

class FunctionBuilder;

/** Translates CSV scan plans. */
class CSVScanTranslator : public OperatorTranslator {
 public:
  /**
   * Create a new translator for the given scan plan.
   * @param plan The plan.
   * @param compilation_context The context of compilation this translation is occurring in.
   * @param pipeline The pipeline this operator is participating in.
   */
  CSVScanTranslator(const planner::CSVScanPlanNode &plan, CompilationContext *compilation_context, Pipeline *pipeline);

  /**
   * Define the base row type this scan produces.
   * @param decls The top-level declaration list.
   */
  void DefineHelperStructs(util::RegionVector<ast::StructDecl *> *decls) override;

  /**
   * Generate the CSV scan logic.
   * @param context The context of work.
   * @param function The function being built.
   */
  void PerformPipelineWork(WorkContext *context, FunctionBuilder *function) const override;

  /**
   * Access a column from the base CSV.
   * @param col_oid The ID of the column to read.
   * @return The value of the column.
   */
  ast::Expr *GetTableColumn(catalog::col_oid_t col_oid) const override;

 private:
  // Return the plan.
  const planner::CSVScanPlanNode &GetCSVPlan() const { return GetPlanAs<planner::CSVScanPlanNode>(); }

  // Access the given field in the CSV row.
  ast::Expr *GetField(uint32_t field_index) const;
  // Access a pointer to the field in the CSV row.
  ast::Expr *GetFieldPtr(uint32_t field_index) const;

 private:
  // The name of the base row variable.
  ast::Identifier base_row_type_;
  StateDescriptor::Entry base_row_;
};

}  // namespace noisepage::execution::compiler
