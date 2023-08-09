/*
 * @Author: sunyixing22 1400945253@qq.com
 * @Date: 2023-06-08 21:48:20
 * @LastEditors: sunyixing22 1400945253@qq.com
 * @LastEditTime: 2023-08-09 15:12:10
 * @FilePath: /bustub-20221128-2022fall/src/execution/limit_executor.cpp
 * @Description: 这是默认设置,请设置`customMade`, 打开koroFileHeader查看配置 进行设置:
 * https://github.com/OBKoro1/koro1FileHeader/wiki/%E9%85%8D%E7%BD%AE
 */
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {}

void LimitExecutor::Init() {
  child_executor_->Init();
  count_ = 0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (count_ >= plan_->GetLimit()) {
    return false;
  }

  if (!child_executor_->Next(tuple, rid)) {
    return false;
  }

  count_++;
  return true;
}

}  // namespace bustub
