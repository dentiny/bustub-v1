//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.h
//
// Identification: src/include/execution/executors/update_executor.h
//
// Copyright (c) 2015-20, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/update_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * UpdateExecutor executes an update in a table.
 * Updated values from a child executor.
 */
class UpdateExecutor : public AbstractExecutor {
  friend class UpdatePlanNode;

 public:
  /**
   * Creates a new update executor.
   * @param exec_ctx the executor context
   * @param plan the update plan to be executed
   */
  UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                 std::unique_ptr<AbstractExecutor> &&child_executor);

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

  void Init() override;

  bool Next([[maybe_unused]] Tuple *tuple, RID *rid) override;

  /*
   * Given an old tuple, creates a new updated tuple based on the updateinfo given in the plan
   * @param old_tup the tuple to be updated
   */
  Tuple GenerateUpdatedTuple(const Tuple &old_tup) {
    auto update_attrs = plan_->GetUpdateAttr();
    Schema schema = table_info_->schema_;
    uint32_t col_count = schema.GetColumnCount();
    std::vector<Value> values;
    for (uint32_t idx = 0; idx < col_count; idx++) {
      if (update_attrs->find(idx) == update_attrs->end()) {
        values.emplace_back(old_tup.GetValue(&schema, idx));
      } else {
        UpdateInfo info = update_attrs->at(idx);
        Value val = old_tup.GetValue(&schema, idx);
        switch (info.type_) {
          case UpdateType::Add:
            values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
            break;

          case UpdateType::Set:
            values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
            break;
        }
      }
    }
    return Tuple(values, &schema);
  }

 private:
  // Iterate all attributes, and find whether it's within update plan.
  bool IsUpdateIndex(const std::vector<uint32_t> &key_attrs) {
    auto *update_attrs = plan_->GetUpdateAttr();
    for (uint32_t key_attr : key_attrs) {
      if (update_attrs->find(key_attr) != update_attrs->end()) {
        return true;
      }
    }
    return false;
  }

  void UpdateIndex(Tuple *old_tuple, Tuple *new_tuple, const RID &rid) {
    for (const auto &index_info : table_indexes_) {
      Index *index = index_info->index_.get();
      if (IsUpdateIndex(index->GetKeyAttrs())) {
        Schema schema = table_info_->schema_;
        Tuple old_key = old_tuple->KeyFromTuple(schema, index_info->key_schema_, index->GetKeyAttrs());
        Tuple new_key = new_tuple->KeyFromTuple(schema, index_info->key_schema_, index->GetKeyAttrs());
        index->DeleteEntry(old_key, rid, exec_ctx_->GetTransaction());
        index->InsertEntry(new_key, rid, exec_ctx_->GetTransaction());
      }
    }
  }

 private:
  /** The update plan node to be executed. */
  const UpdatePlanNode *plan_;
  /** Metadata identifying the table that should be updated. */
  const TableMetadata *table_info_;
  /** The child executor to obtain value from. */
  std::unique_ptr<AbstractExecutor> child_executor_;
  /** Index to update */
  std::vector<IndexInfo *> table_indexes_;
};
}  // namespace bustub
