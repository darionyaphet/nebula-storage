/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_DISKBALANCEJOBEXECUTOR_H_
#define META_DISKBALANCEJOBEXECUTOR_H_

#include "meta/processors/jobMan/BalanceTask.h"
#include "meta/processors/jobMan/BalancePlan.h"
#include "meta/processors/jobMan/MetaJobExecutor.h"
#include <folly/executors/CPUThreadPoolExecutor.h>

namespace nebula {
namespace meta {

class DiskBalanceJobExecutor : public MetaJobExecutor {
public:
    DiskBalanceJobExecutor(JobID jobId,
                           kvstore::KVStore* kvstore,
                           AdminClient* adminClient,
                           const std::vector<std::string>& params);

    bool check() override;

    cpp2::ErrorCode prepare() override;

    cpp2::ErrorCode execute() override;

    cpp2::ErrorCode stop() override;

    void finish(bool /*successed*/) override {}

protected:
    folly::Future<Status>
    executeInternal(HostAddr&&, std::vector<PartitionID>&&) override {
        return Status::OK();
    }

private:
    ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
    genTasks();

    bool balanceDisk(std::vector<BalanceTask>& tasks);

private:
std::shared_ptr<BalancePlan>            plan_;
    std::unique_ptr<folly::Executor>    executor_;
    std::vector<HostAddr>               hosts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_DISKBALANCEJOBEXECUTOR_H_
