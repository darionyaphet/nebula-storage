/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_QUERY_SCANPROCESSOR_H_
#define STORAGE_QUERY_SCANPROCESSOR_H_

#include "storage/query/QueryBaseProcessor.h"

namespace nebula {
namespace storage {

extern ProcessorCounters kScanCounters;

class ScanProcessor
    : public QueryBaseProcessor<cpp2::ScanRequest, cpp2::ScanResponse> {
public:
    static ScanProcessor* instance(StorageEnv* env,
                                   const ProcessorCounters* counters = &kScanCounters,
                                   folly::Executor* executor = nullptr,
                                   VertexCache* cache = nullptr) {
        return new ScanProcessor(env, counters, executor, cache);
    }

    void process(const cpp2::ScanRequest& req) override;

    void doProcess(const cpp2::ScanRequest& req);

private:
    ScanProcessor(StorageEnv* env,
                  const ProcessorCounters* counters,
                  folly::Executor* executor,
                  VertexCache* cache)
        : QueryBaseProcessor<cpp2::ScanRequest, cpp2::ScanResponse>(env,
                                                                    counters,
                                                                    executor,
                                                                    cache) {}

    cpp2::ErrorCode checkAndBuildContexts(const cpp2::ScanRequest& req) override;

    void buildColumnName(const std::vector<cpp2::SchemaProp>& property);

    void onProcessFinished() override;

private:
    bool        isEdge_;
    PartitionID partId_;
};

}  // namespace storage
}  // namespace nebula
#endif  // STORAGE_QUERY_SCANPROCESSOR_H_
