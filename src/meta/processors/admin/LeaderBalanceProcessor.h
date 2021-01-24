/* Copyright (c) 2019 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef META_LEADERCOUNTPROCESSOR_H_
#define META_LEADERCOUNTPROCESSOR_H_

#include "meta/processors/BaseProcessor.h"
#include "meta/processors/admin/AdminClient.h"

namespace nebula {
namespace meta {

using SpaceInfo = std::pair<int32_t, bool>;
using HostParts = std::unordered_map<HostAddr, std::vector<PartitionID>>;
using PartAllocation = std::unordered_map<PartitionID, std::vector<HostAddr>>;
using LeaderBalancePlan = std::vector<std::tuple<GraphSpaceID, PartitionID, HostAddr, HostAddr>>;
using ZoneNameAndParts = std::pair<std::string, std::vector<PartitionID>>;

class LeaderBalanceProcessor : public BaseProcessor<cpp2::ExecResp> {
    FRIEND_TEST(BalanceLeaderTest, SimpleLeaderBalancePlanTest);
    FRIEND_TEST(BalanceLeaderTest, IntersectHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceLeaderTest, ManyHostsLeaderBalancePlanTest);
    FRIEND_TEST(BalanceLeaderTest, LeaderBalanceTest);
    FRIEND_TEST(BalanceLeaderTest, LeaderBalanceWithZoneTest);
    FRIEND_TEST(BalanceLeaderTest, LeaderBalanceWithLargerZoneTest);
    FRIEND_TEST(BalanceLeaderTest, LeaderBalanceWithComplexZoneTest);

public:
    static LeaderBalanceProcessor* instance(kvstore::KVStore* kvstore) {
        return new LeaderBalanceProcessor(kvstore);
    }

    void process(const cpp2::LeaderBalanceReq& req);

private:
    explicit LeaderBalanceProcessor(kvstore::KVStore* kvstore)
        : BaseProcessor<cpp2::ExecResp>(kvstore) {
        client_ = std::make_unique<AdminClient>(kvstore);
    }

    cpp2::ErrorCode leaderBalance();

    bool getHostParts(bool dependentOnGroup,
                      HostParts& hostParts,
                      int32_t& totalParts);

    bool assembleZoneParts(const std::string& groupName, HostParts& hostParts);

    bool buildLeaderBalancePlan(HostLeaderMap* hostLeaderMap,
                                int32_t replicaFactor,
                                bool dependentOnGroup,
                                LeaderBalancePlan& plan,
                                bool useDeviation = true);

    int32_t acquireLeaders(HostParts& allHostParts,
                           HostParts& leaderHostParts,
                           PartAllocation& peersMap,
                           std::unordered_set<HostAddr>& activeHosts,
                           const HostAddr& target,
                           LeaderBalancePlan& plan);

    int32_t giveupLeaders(HostParts& leaderHostParts,
                          PartAllocation& peersMap,
                          std::unordered_set<HostAddr>& activeHosts,
                          const HostAddr& source,
                          LeaderBalancePlan& plan);

    void simplifyLeaderBalnacePlan(LeaderBalancePlan& plan);

    void calculateHostBounds(bool dependentOnGroup,
                             HostParts allHostParts,
                             int32_t replicaFactor,
                             bool useDeviation,
                             size_t leaderParts,
                             std::unordered_set<HostAddr> activeHosts);

    int32_t calculateLeaderParts(PartAllocation& peersMap);

private:
    GraphSpaceID space_;
    std::unique_ptr<AdminClient> client_{nullptr};

    // Host => Graph => Partitions
    std::unique_ptr<HostLeaderMap> hostLeaderMap_;
    std::unordered_map<HostAddr, std::pair<int32_t, int32_t>> hostBounds_;
    std::unordered_map<HostAddr, ZoneNameAndParts> zoneParts_;
};

}  // namespace meta
}  // namespace nebula

#endif  // META_LEADERCOUNTPROCESSOR_H_
