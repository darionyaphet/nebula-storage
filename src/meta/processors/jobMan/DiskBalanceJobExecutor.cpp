/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/ActiveHostsMan.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/jobMan/DiskBalanceJobExecutor.h"

namespace nebula {
namespace meta {

DiskBalanceJobExecutor::DiskBalanceJobExecutor(JobID jobId,
                                               kvstore::KVStore* kvstore,
                                               AdminClient* adminClient,
                                               const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
    executor_.reset(new folly::CPUThreadPoolExecutor(1));
}

bool DiskBalanceJobExecutor::check() {
    return paras_.size() == 2;
}

cpp2::ErrorCode DiskBalanceJobExecutor::prepare() {
    auto spaceRet = getSpaceIdFromName(paras_.back());
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_.back();
        return nebula::error(spaceRet);
    }

    space_ = nebula::value(spaceRet);
    std::vector<std::string> addresses;
    folly::split(',', paras_.front(), addresses);
    for (const auto& address : addresses) {
        std::vector<std::string> token;
        folly::split(':', address, token);
        if (token.size() != 2) {
            LOG(ERROR) << "address format error";
            return cpp2::ErrorCode::E_INVALID_PARM;
        }

        Port port;
        try {
            port = folly::to<Port>(token[1]);
        } catch (const std::exception& ex) {
            LOG(ERROR) << "Port number error: " << ex.what();
            return cpp2::ErrorCode::E_INVALID_PARM;
        }
        hosts_.emplace_back(token[0], port);
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode DiskBalanceJobExecutor::execute() {
    auto spaceInfoRet = MetaCommon::getSpaceInfo(kvstore_, space_);
    if (!spaceInfoRet.ok()) {
        LOG(ERROR) << folly::sformat("Can't get space {}", space_);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    auto taskRet = genTasks();
    if (!ok(taskRet)) {
        LOG(ERROR) << folly::sformat("Generate tasks on space {} failed", space_);
        return error(taskRet);
    }

    auto tasks = std::move(value(taskRet));
    for (auto& task : tasks) {
        plan_->addTask(std::move(task));
    }

    if (plan_->tasks().empty()) {
        LOG(INFO) << "Current space " << space_ << " balanced";
        plan_->status_ = cpp2::JobStatus::FINISHED;
        finish(true);
        return plan_->saveJobStatus();
    }

    plan_->onFinished_ = [this] () {
        auto now = time::WallClock::fastNowInMilliSec();
        if (LastUpdateTimeMan::update(kvstore_, now) != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << folly::sformat("Balance plan {} update meta failed", plan_->id());
        }
        finish(true);
        plan_->saveJobStatus();
    };

    LOG(INFO) << "Start to invoke balance plan " << plan_->id();
    executor_->add(std::bind(&BalancePlan::invoke, plan_.get()));
    return plan_->saveJobStatus();
}

ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
DiskBalanceJobExecutor::genTasks() {
    std::vector<BalanceTask> tasks;

    for (const auto& host : hosts_) {
        LOG(INFO) << "Balance host: " << host;
    }
    return tasks;
}

bool DiskBalanceJobExecutor::balanceDisk(std::vector<BalanceTask>& tasks) {
    LOG(INFO) << "Disk Balance tasks num: " << tasks.size();
    for (auto& task : tasks) {
        LOG(INFO) << "Disk  Balance Task: " << task.taskIdStr();
    }
    return true;
}

cpp2::ErrorCode DiskBalanceJobExecutor::stop() {
    return cpp2::ErrorCode::SUCCEEDED;
}

}  // namespace meta
}  // namespace nebula
