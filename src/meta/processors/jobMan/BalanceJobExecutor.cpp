/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "meta/MetaServiceUtils.h"
#include "meta/ActiveHostsMan.h"
#include "meta/common/MetaCommon.h"
#include "meta/processors/jobMan/BalanceJobExecutor.h"

namespace nebula {
namespace meta {

BalanceJobExecutor::BalanceJobExecutor(JobID jobId,
                                       kvstore::KVStore* kvstore,
                                       AdminClient* adminClient,
                                       const std::vector<std::string>& paras)
    : MetaJobExecutor(jobId, kvstore, adminClient, paras) {
        executor_.reset(new folly::CPUThreadPoolExecutor(1));
    }


bool BalanceJobExecutor::check() {
    return paras_.size() == 1 || paras_.size() == 2;
}

cpp2::ErrorCode BalanceJobExecutor::prepare() {
    auto spaceRet = getSpaceIdFromName(paras_.back());
    if (!nebula::ok(spaceRet)) {
        LOG(ERROR) << "Can't find the space: " << paras_.back();
        return nebula::error(spaceRet);
    }

    space_ = nebula::value(spaceRet);
    if (paras_.size() == 2) {
        LOG(INFO) << "Remove addresses: " << paras_.front();
        std::vector<std::string> addresses;
        folly::split(',', paras_.front(), addresses);
        for (const auto& address : addresses) {
            LOG(INFO) << "Remove address: " << address;
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
            lostHosts_.emplace_back(token[0], port);
        }
    }
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode BalanceJobExecutor::stop() {
    return cpp2::ErrorCode::SUCCEEDED;
}

cpp2::ErrorCode BalanceJobExecutor::execute() {
    auto spaceInfoRet = MetaCommon::getSpaceInfo(kvstore_, space_);
    if (!spaceInfoRet.ok()) {
        LOG(ERROR) << folly::sformat("Can't get space {}", space_);
        return cpp2::ErrorCode::E_STORE_FAILURE;
    }

    auto spaceInfo = spaceInfoRet.value();
    auto spaceReplica = spaceInfo.first;
    auto dependentOnGroup = spaceInfo.second;
    LOG(INFO) << folly::sformat("Balance Space {}", space_);
    auto taskRet = genTasks(spaceReplica, dependentOnGroup, lostHosts_);
    if (!ok(taskRet)) {
        LOG(ERROR) << folly::sformat("Generate tasks on space {} failed", space_);
        return error(taskRet);
    }

    auto tasks = std::move(value(taskRet));
    plan_ = std::make_unique<BalancePlan>(jobId_, space_, kvstore_, adminClient_);
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

bool BalanceJobExecutor::getHostParts(bool dependentOnGroup,
                                      HostParts& hostParts,
                                      int32_t& totalParts) {
    folly::SharedMutex::ReadHolder rHolder(LockUtils::spaceLock());
    auto prefix = MetaServiceUtils::partPrefix(space_);
    std::unique_ptr<kvstore::KVIterator> iter;
    auto code = kvstore_->prefix(kDefaultSpaceId, kDefaultPartId, prefix, &iter);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << folly::sformat("Access kvstore failed, spaceId {}", space_);
        return false;
    }

    while (iter->valid()) {
        auto key = iter->key();
        PartitionID partId;
        memcpy(&partId, key.data() + prefix.size(), sizeof(PartitionID));
        auto partHosts = MetaServiceUtils::parsePartVal(iter->val());
        for (auto& ph : partHosts) {
            hostParts[ph].emplace_back(partId);
        }
        totalParts++;
        iter->next();
    }

    LOG(INFO) << "Host size: " << hostParts.size();
    auto key = MetaServiceUtils::spaceKey(space_);
    std::string value;
    code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, key, &value);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << folly::sformat("Access kvstore failed, spaceId {}", space_);
        return false;
    }

    auto properties = MetaServiceUtils::parseSpace(value);
    if (totalParts != properties.get_partition_num()) {
        LOG(ERROR) << "Partition number not equals";
        return false;
    }

    if (properties.__isset.group_name) {
        auto groupName = *properties.get_group_name();
        if (dependentOnGroup && !assembleZoneParts(groupName, hostParts)) {
            LOG(ERROR) << folly::sformat("Assemble Zone Parts failed group: {}", groupName);
            return false;
        }
    }

    totalParts *= properties.get_replica_factor();
    return true;
}

std::pair<HostParts, std::vector<HostAddr>>
BalanceJobExecutor::fetchHostParts(bool dependentOnGroup,
                                   const HostParts& hostParts,
                                   std::vector<HostAddr>& lostHosts) {
    std::vector<HostAddr> activeHosts;
    if (dependentOnGroup) {
        activeHosts = ActiveHostsMan::getActiveHostsWithGroup(kvstore_, space_);
    } else {
        activeHosts = ActiveHostsMan::getActiveHosts(kvstore_);
    }

    std::vector<HostAddr> expand;
    calDiff(hostParts, activeHosts, expand, lostHosts);
    // confirmedHostParts is new part allocation map after balance, it would include newlyAdded
    // and exclude lostHosts
    HostParts confirmedHostParts(hostParts);
    for (const auto& h : expand) {
        LOG(INFO) << folly::sformat("Found new host: {}", h.toString());
        confirmedHostParts.emplace(h, std::vector<PartitionID>());
    }
    for (const auto& h : lostHosts) {
        LOG(INFO) << folly::sformat("Lost host: {}", h.toString());
        confirmedHostParts.erase(h);
    }
    return std::make_pair(confirmedHostParts, activeHosts);
}

ErrorOr<cpp2::ErrorCode, std::vector<BalanceTask>>
BalanceJobExecutor::genTasks(int32_t spaceReplica,
                             bool dependentOnGroup,
                             std::vector<HostAddr> lostHosts) {
    HostParts hostParts;
    int32_t totalParts = 0;
    // hostParts is current part allocation map
    auto result = getHostParts(dependentOnGroup, hostParts, totalParts);
    if (!result || totalParts == 0 || hostParts.empty()) {
        LOG(ERROR) << folly::sformat("Invalid space: {}", space_);
        return cpp2::ErrorCode::E_NOT_FOUND;
    }

    auto hostPartsRet = fetchHostParts(dependentOnGroup, hostParts, lostHosts);
    auto confirmedHostParts = hostPartsRet.first;
    auto activeHosts = hostPartsRet.second;
    LOG(INFO) << "Now, try to balance the confirmedHostParts";

    // We have two parts need to balance, the first one is parts on lost hosts and deleted hosts
    // The seconds one is parts on unbalanced host in confirmedHostParts.
    std::vector<BalanceTask> tasks;
    // 1. Iterate through all hosts that would not be included in confirmedHostParts,
    //    move all parts in them to host with minimum part in confirmedHostParts
    for (auto& lostHost : lostHosts) {
        auto& lostParts = hostParts[lostHost];
        for (auto& partId : lostParts) {
            LOG(INFO) << folly::sformat("Try balance part {} for lost host {}",
                                        partId, lostHost.toString());
            // check whether any peers which is alive
            auto alive = checkReplica(hostParts, activeHosts, spaceReplica, partId);
            if (!alive.ok()) {
                LOG(ERROR) << folly::sformat("Check Replica failed: {} Part: {}",
                                             alive.toString(), partId);
                return cpp2::ErrorCode::E_NO_VALID_HOST;
            }

            if (!transferLostHost(tasks, confirmedHostParts, lostHost,
                                  partId, dependentOnGroup)) {
                LOG(ERROR) << folly::sformat("Transfer lost host {} failed", lostHost.toString());
                return cpp2::ErrorCode::E_NO_VALID_HOST;
            }
        }
    }

    if (confirmedHostParts.size() < 2) {
        LOG(INFO) << "Too few hosts, no need for balance!";
        return cpp2::ErrorCode::E_NO_VALID_HOST;
    }
    // 2. Make all hosts in confirmedHostParts balanced
    if (balanceParts(jobId_, confirmedHostParts, totalParts, tasks)) {
        return tasks;
    } else {
        return cpp2::ErrorCode::E_BAD_BALANCE_PLAN;
    }
}

void BalanceJobExecutor::calDiff(const HostParts& hostParts,
                                 const std::vector<HostAddr>& activeHosts,
                                 std::vector<HostAddr>& expand,
                                 std::vector<HostAddr>& lost) {
    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        VLOG(1) << "Original Host " << it->first << ", parts " << it->second.size();
        if (std::find(activeHosts.begin(), activeHosts.end(), it->first) == activeHosts.end() &&
            std::find(lost.begin(), lost.end(), it->first) == lost.end()) {
            lost.emplace_back(it->first);
        }
    }
    for (auto& h : activeHosts) {
        VLOG(1) << "Active host " << h;
        if (hostParts.find(h) == hostParts.end()) {
            expand.emplace_back(h);
        }
    }
}

Status BalanceJobExecutor::checkReplica(const HostParts& hostParts,
                                        const std::vector<HostAddr>& activeHosts,
                                        int32_t replica,
                                        PartitionID partId) {
    // check host hold the part and alive
    auto checkPart = [&] (const auto& entry) {
        const auto& host = entry.first;
        const auto& parts = entry.second;
        return std::find(parts.begin(), parts.end(), partId) != parts.end() &&
               std::find(activeHosts.begin(), activeHosts.end(), host) != activeHosts.end();
    };
    auto aliveReplica = std::count_if(hostParts.begin(), hostParts.end(), checkPart);
    if (aliveReplica >= replica / 2 + 1) {
        return Status::OK();
    }
    return Status::Error("Not enough alive host hold the part %d", partId);
}

std::vector<std::pair<HostAddr, int32_t>>
BalanceJobExecutor::sortedHostsByParts(const HostParts& hostParts) {
    std::vector<std::pair<HostAddr, int32_t>> hosts;
    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        hosts.emplace_back(it->first, it->second.size());
    }
    std::sort(hosts.begin(), hosts.end(), [](const auto& l, const auto& r) {
        return l.second < r.second;
    });
    return hosts;
}

StatusOr<HostAddr> BalanceJobExecutor::hostWithMinimalParts(const HostParts& hostParts,
                                                            PartitionID partId) {
    auto hosts = sortedHostsByParts(hostParts);
    for (auto& h : hosts) {
        auto it = hostParts.find(h.first);
        if (it == hostParts.end()) {
            LOG(ERROR) << "Host " << h.first << " not found";
            return Status::Error("Host not found");
        }

        if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end()) {
            return h.first;
        }
    }
    return Status::Error("No host is suitable for %d", partId);
}

StatusOr<HostAddr> BalanceJobExecutor::hostWithMinimalPartsForZone(const HostAddr& source,
                                                                   const HostParts& hostParts,
                                                                   PartitionID partId) {
    auto hosts = sortedHostsByParts(hostParts);
    for (auto& h : hosts) {
        auto it = hostParts.find(h.first);
        if (it == hostParts.end()) {
            LOG(ERROR) << "Host " << h.first << " not found";
            return Status::Error("Host not found");
        }

        if (std::find(it->second.begin(), it->second.end(), partId) == it->second.end() &&
            checkZoneLegal(source, h.first, partId)) {
            return h.first;
        }
    }
    return Status::Error("No host is suitable for %d", partId);
}

bool BalanceJobExecutor::checkZoneLegal(const HostAddr& source,
                                        const HostAddr& target,
                                        PartitionID part) {
    VLOG(3) << "Check " << source << " : " << target << " with part " << part;
    auto sourceIter = std::find_if(zoneParts_.begin(), zoneParts_.end(),
                                   [&source](const auto& pair) {
        return source == pair.first;
    });

    if (sourceIter == zoneParts_.end()) {
        LOG(INFO) << "Source " << source << " not found";
        return false;
    }

    auto targetIter = std::find_if(zoneParts_.begin(), zoneParts_.end(),
                                   [&target](const auto& pair) {
        return target == pair.first;
    });

    if (targetIter == zoneParts_.end()) {
        LOG(INFO) << "Target " << target << " not found";
        return false;
    }

    if (sourceIter->second.first == targetIter->second.first) {
        LOG(INFO) << source << " --> " << target << " transfer in the same zone";
        return true;
    }

    auto& parts = targetIter->second.second;
    return std::find(parts.begin(), parts.end(), part) == parts.end();
}

bool BalanceJobExecutor::balanceParts(JobID id,
                                      HostParts& confirmedHostParts,
                                      int32_t totalParts,
                                      std::vector<BalanceTask>& tasks) {
    auto avgLoad = static_cast<float>(totalParts) / confirmedHostParts.size();
    VLOG(3) << "The expect avg load is " << avgLoad;
    int32_t minLoad = std::floor(avgLoad);
    int32_t maxLoad = std::ceil(avgLoad);
    VLOG(3) << "The min load is " << minLoad << " max load is " << maxLoad;

    auto sortedHosts = sortedHostsByParts(confirmedHostParts);
    if (sortedHosts.empty()) {
        LOG(ERROR) << "Host is empty";
        return false;
    }

    auto maxPartsHost = sortedHosts.back();
    auto minPartsHost = sortedHosts.front();

    while (maxPartsHost.second > maxLoad || minPartsHost.second < minLoad) {
        auto& partsFrom = confirmedHostParts[maxPartsHost.first];
        auto& partsTo = confirmedHostParts[minPartsHost.first];
        std::sort(partsFrom.begin(), partsFrom.end());
        std::sort(partsTo.begin(), partsTo.end());

        LOG(INFO) << maxPartsHost.first << ":" << partsFrom.size()
                  << " -> " << minPartsHost.first << ":" << partsTo.size();
        std::vector<PartitionID> diff;
        std::set_difference(partsFrom.begin(), partsFrom.end(), partsTo.begin(), partsTo.end(),
                            std::inserter(diff, diff.begin()));
        bool noAction = true;
        for (auto& partId : diff) {
            LOG(INFO) << "partsFrom size " << partsFrom.size()
                      << " partsTo size " << partsTo.size()
                      << " minLoad " << minLoad << " maxLoad " << maxLoad;
            if (partsFrom.size() == partsTo.size() + 1 ||
                partsFrom.size() == static_cast<size_t>(minLoad) ||
                partsTo.size() == static_cast<size_t>(maxLoad)) {
                VLOG(3) << "No need to move any parts from "
                        << maxPartsHost.first << " to " << minPartsHost.first;
                break;
            }

            LOG(INFO) << "[space:" << space_ << ", part:" << partId << "] "
                      << maxPartsHost.first << "->" << minPartsHost.first;
            auto it = std::find(partsFrom.begin(), partsFrom.end(), partId);
            if (it == partsFrom.end()) {
                LOG(ERROR) << "Part " << partId << " not found in partsFrom";
                return false;
            }

            partsFrom.erase(it);
            if (std::find(partsTo.begin(), partsTo.end(), partId) != partsTo.end()) {
                LOG(ERROR) << "Part " << partId << " already existed in partsTo";
                return false;
            }

            partsTo.emplace_back(partId);
            tasks.emplace_back(id,
                               space_,
                               partId,
                               maxPartsHost.first,
                               minPartsHost.first,
                               kvstore_,
                               adminClient_);
            noAction = false;
        }
        if (noAction) {
            LOG(INFO) << "Here is no action";
            break;
        }
        sortedHosts = sortedHostsByParts(confirmedHostParts);
        maxPartsHost = sortedHosts.back();
        minPartsHost = sortedHosts.front();
    }

    LOG(INFO) << "Balance tasks num: " << tasks.size();
    for (auto& task : tasks) {
        LOG(INFO) << "Balance Task: " << task.taskIdStr();
    }
    return true;
}

bool BalanceJobExecutor::transferLostHost(std::vector<BalanceTask>& tasks,
                                          HostParts& confirmedHostParts,
                                          const HostAddr& source,
                                          PartitionID partId,
                                          bool dependentOnGroup) {
    // find a host with minimum parts which doesn't have this part
    StatusOr<HostAddr> result;
    if (dependentOnGroup) {
        result = hostWithMinimalPartsForZone(source, confirmedHostParts, partId);
    } else {
        result = hostWithMinimalParts(confirmedHostParts, partId);
    }

    if (!result.ok()) {
        LOG(ERROR) << "Can't find a host which doesn't have part: " << partId;
        return false;
    }
    auto targetHost = result.value();
    confirmedHostParts[targetHost].emplace_back(partId);
    tasks.emplace_back(jobId_,
                       space_,
                       partId,
                       source,
                       targetHost,
                       kvstore_,
                       adminClient_);
    return true;
}

bool BalanceJobExecutor::assembleZoneParts(const std::string& groupName, HostParts& hostParts) {
    auto groupKey = MetaServiceUtils::groupKey(groupName);
    std::string groupValue;
    auto code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, groupKey, &groupValue);
    if (code != kvstore::ResultCode::SUCCEEDED) {
        LOG(ERROR) << "Get group " << groupName << " failed";
        return false;
    }

    // zoneHosts use to record this host belong to zone's hosts
    std::unordered_map<std::pair<HostAddr, std::string>, std::vector<HostAddr>> zoneHosts;
    auto zoneNames = MetaServiceUtils::parseZoneNames(std::move(groupValue));
    for (auto zoneName : zoneNames) {
        auto zoneKey = MetaServiceUtils::zoneKey(zoneName);
        std::string zoneValue;
        code = kvstore_->get(kDefaultSpaceId, kDefaultPartId, zoneKey, &zoneValue);
        if (code != kvstore::ResultCode::SUCCEEDED) {
            LOG(ERROR) << "Get zone " << zoneName << " failed";
            return false;
        }

        auto hosts = MetaServiceUtils::parseZoneHosts(std::move(zoneValue));
        for (const auto& host : hosts) {
            auto pair = std::pair<HostAddr, std::string>(std::move(host),
                                                         std::move(zoneName));
            auto& hs = zoneHosts[std::move(pair)];
            hs.insert(hs.end(), hosts.begin(), hosts.end());
        }
    }

    for (auto it = hostParts.begin(); it != hostParts.end(); it++) {
        auto host = it->first;
        auto zoneIter = std::find_if(zoneHosts.begin(), zoneHosts.end(),
                                     [host](const auto& pair) -> bool {
            return host == pair.first.first;
        });

        if (zoneIter == zoneHosts.end()) {
            LOG(INFO) << it->first << " have lost";
            continue;
        }

        auto& hosts = zoneIter->second;
        auto name = zoneIter->first.second;
        for (auto hostIter = hosts.begin(); hostIter != hosts.end(); hostIter++) {
            auto partIter = hostParts.find(*hostIter);
            zoneParts_[it->first] = ZoneNameAndParts(name, partIter->second);
        }
    }
    return true;
}

}  // namespace meta
}  // namespace nebula

