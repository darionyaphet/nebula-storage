/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#ifndef STORAGE_EXEC_SCANNODE_H_
#define STORAGE_EXEC_SCANNODE_H_

namespace nebula {
namespace storage {

class ScanTagNode : public QueryNode<VertexID> {
public:
    using RelNode<VertexID>::execute;

    explicit ScanTagNode(PlanContext *planCtx,
                         std::vector<TagNode*> tagNodes,
                         nebula::DataSet* resultDataSet,
                         VertexCache* vertexCache)
        : planContext_(planCtx)
        , tagNodes_(std::move(tagNodes))
        , resultDataSet_(resultDataSet)
        , vertexCache_(vertexCache) {}

    kvstore::ResultCode execute(PartitionID partId, const VertexID& vId) override {
        auto ret = RelNode::execute(partId, vId);
    }
};

class ScanEdgeNode : public QueryNode<cpp2::EdgeKey> {
public:
    using RelNode::execute;

    ScanEdgeNode(PlanContext *planCtx,
                 std::vector<EdgeNode<cpp2::EdgeKey>*> edgeNodes,
                 nebula::DataSet* resultDataSet)
        : planContext_(planCtx)
        , edgeNodes_(std::move(edgeNodes))
        , resultDataSet_(resultDataSet) {}

    kvstore::ResultCode execute(PartitionID partId, const cpp2::EdgeKey& edgeKey) override {
        auto ret = RelNode::execute(partId, edgeKey);
        if (ret != kvstore::ResultCode::SUCCEEDED) {
            return ret;
        }

        return kvstore::ResultCode::SUCCEEDED;
    }

private:
};

}  // namespace storage
}  // namespace nebula

#endif  // STORAGE_EXEC_SCANNODE_H_
