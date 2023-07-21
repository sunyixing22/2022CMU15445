/*
 * @Author: sunyixing22 1400945253@qq.com
 * @Date: 2023-06-08 21:48:20
 * @LastEditors: sunyixing22 1400945253@qq.com
 * @LastEditTime: 2023-07-18 19:32:23
 * @FilePath: /bustub-20221128-2022fall/src/execution/seq_scan_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "common/exception.h"
#include "concurrency/lock_manager.h"
#include "concurrency/transaction.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {
  this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}

void SeqScanExecutor::Init() {
  //   if (exec_ctx_->GetTransaction()->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
  //     try {
  //       bool is_locked = exec_ctx_->GetLockManager()->LockTable(
  //           exec_ctx_->GetTransaction(), LockManager::LockMode::INTENTION_SHARED, table_info_->oid_);
  //       if (!is_locked) {
  //         throw ExecutionException("SeqScan Executor Get Table Lock Failed");
  //       }
  //     } catch (TransactionAbortException &e) {
  //       throw ExecutionException("SeqScan Executor Get Table Lock Failed" + e.GetInfo());
  //     }
  //   }
  this->table_iter_ = table_info_->table_->Begin(exec_ctx_->GetTransaction());
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  //   do {
  //     if (table_iter_ == table_info_->table_->End()) {
  //       if (exec_ctx_->GetTransaction()->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
  //       }
  //     }
  //   }

  if (table_iter_ != table_info_->table_->End()) {
    *tuple = *table_iter_;
    *rid = table_iter_->GetRid();
    ++table_iter_;
    return true;
  }
  return false;
}

}  // namespace bustub
