/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_JOB_DISKBALANCETASK_H_
#define META_JOB_DISKBALANCETASK_H_

namespace nebula {
namespace meta {

class DiskBalanceTask {
public:
    DiskBalanceTask() = default;

    DiskBalanceTask(JobID id,
                    GraphSpaceID spaceId,
                    PartitionID partId,
                    const std::string& src,
                    const std::string& dst,
                    kvstore::KVStore* kv,
                    AdminClient* client)
        : id_(id)
        , spaceId_(spaceId)
        , partId_(partId)
        , src_(src)
        , dst_(dst)
        , taskIdStr_(buildTaskId())
        , kv_(kv)
        , client_(client) {}

public:
    JobID        id_;
    GraphSpaceID spaceId_;
    PartitionID  partId_;
    std::string  src_;
    std::string  dst_;
    std::string  taskIdStr_;
    kvstore::KVStore* kv_ = nullptr;
    AdminClient* client_ = nullptr;
    BalanceTaskStatus status_ = BalanceTaskStatus::START;
    BalanceTaskResult ret_ = BalanceTaskResult::IN_PROGRESS;
    int64_t startTimeMs_ = 0;
    int64_t endTimeMs_ = 0;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_JOB_DISKBALANCETASK_H_
