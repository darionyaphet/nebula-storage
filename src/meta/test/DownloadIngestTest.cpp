/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include <gtest/gtest.h>
#include "common/base/Base.h"
#include "common/fs/TempDir.h"
#include "kvstore/Common.h"
#include "meta/processors/jobMan/JobUtils.h"
#include "meta/processors/jobMan/JobManager.h"
#include "meta/test/TestUtils.h"

namespace nebula {
namespace meta {

class DownloadIngestTest : public ::testing::Test {
protected:
    void SetUp() override {
        rootPath_ = std::make_unique<fs::TempDir>("/tmp/JobManager.XXXXXX");
        mock::MockCluster cluster;
        kv_ = cluster.initMetaKV(rootPath_->path());

        ASSERT_TRUE(TestUtils::createSomeHosts(kv_.get()));
        TestUtils::assembleSpace(kv_.get(), 1, 1);
    }

    void TearDown() override {
        auto cleanUnboundQueue = [](auto& q) {
            int32_t jobId = 0;
            while (!q.empty()) {
                q.dequeue(jobId);
            }
        };
        cleanUnboundQueue(*jobMgr->lowPriorityQueue_);
        cleanUnboundQueue(*jobMgr->highPriorityQueue_);
        kv_.reset();
        rootPath_.reset();
    }

    std::unique_ptr<fs::TempDir> rootPath_{nullptr};
    std::unique_ptr<kvstore::KVStore> kv_{nullptr};
    std::unique_ptr<AdminClient> adminClient_{nullptr};
    JobManager* jobMgr{nullptr};
};

TEST_F(DownloadIngestTest, DownloadIngest) {
}

}  // namespace meta
}  // namespace nebula

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    folly::init(&argc, &argv, true);
    google::SetStderrLogging(google::INFO);
    return RUN_ALL_TESTS();
}
