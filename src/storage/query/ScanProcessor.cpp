/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include "storage/exec/QueryUtils.h"
#include "storage/query/ScanProcessor.h"

namespace nebula {
namespace storage {

ProcessorCounters kScanCounters;

void ScanProcessor::process(const cpp2::ScanRequest& req) {
    auto type = req.get_property().get_schema().getType();
    isEdge_ = type == nebula::cpp2::SchemaID::Type::edge_type;
    if (executor_ != nullptr) {
        executor_->add([req, this] () {
            this->doProcess(req);
        });
    } else {
        doProcess(req);
    }
}

void ScanProcessor::doProcess(const cpp2::ScanRequest& req) {
    spaceId_ = req.get_space_id();
    partId_ = req.get_part_id();

    auto code = getSpaceVidLen(spaceId_);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(code, partId_);
        onFinished();
        return;
    }

    code = checkAndBuildContexts(req);
    if (code != cpp2::ErrorCode::SUCCEEDED) {
        pushResultCode(code, partId_);
        onFinished();
        return;
    }

    std::string start;
    std::string prefix = isEdge_ ? NebulaKeyUtils::edgePrefix(partId_)
                                 : NebulaKeyUtils::vertexPrefix(partId_);
    if (req.get_cursor() == nullptr || req.get_cursor()->empty()) {
        start = prefix;
    } else {
        start = *req.get_cursor();
    }

    std::unique_ptr<kvstore::KVIterator> iter;
    auto result = env_->kvstore_->rangeWithPrefix(spaceId_, partId_,
                                                  start, prefix, &iter,
                                                  req.get_enable_read_from_follower());
    if (result != kvstore::ResultCode::SUCCEEDED) {
        handleErrorCode(result, spaceId_, partId_);
        onFinished();
        return;
    }

    auto rowLimit = req.get_limit();
    RowReaderWrapper reader;

    for (int64_t rowCount = 0; iter->valid() && rowCount < rowLimit; iter->next()) {
        auto key = iter->key();
        auto val = iter->val();
        nebula::List list;
        if (isEdge_) {
            if (!NebulaKeyUtils::isEdge(spaceVidLen_, key)) {
                continue;
            }

            auto edgeType = NebulaKeyUtils::getEdgeType(spaceVidLen_, key);
            auto edgeIter = edgeContext_.indexMap_.find(edgeType);
            if (edgeIter == edgeContext_.indexMap_.end()) {
                continue;
            }

            auto schemaIter = edgeContext_.schemas_.find(std::abs(edgeType));
            CHECK(schemaIter != edgeContext_.schemas_.end());
            reader.reset(schemaIter->second, val);
            if (!reader) {
                continue;
            }

            auto props = &(edgeContext_.propContexts_[edgeIter->second].second);
            if (!QueryUtils::collectEdgeProps(key, spaceVidLen_, isIntId_,
                                              reader.get(), props, list).ok()) {
                continue;
            }
            resultDataSet_.rows.emplace_back(std::move(list));
        } else {
            if (!NebulaKeyUtils::isVertex(spaceVidLen_, key)) {
                continue;
            }
            auto tagId = NebulaKeyUtils::getTagId(spaceVidLen_, key);
            auto tagIter = tagContext_.indexMap_.find(tagId);
            if (tagIter == tagContext_.indexMap_.end()) {
                continue;
            }

            auto schemaIter = tagContext_.schemas_.find(tagId);
            CHECK(schemaIter != tagContext_.schemas_.end());
            reader.reset(schemaIter->second, val);
            if (!reader) {
                continue;
            }

            auto props = &(tagContext_.propContexts_[tagIter->second].second);
            if (!QueryUtils::collectVertexProps(key, spaceVidLen_, isIntId_,
                                                reader.get(), props, list).ok()) {
                continue;
            }
            resultDataSet_.rows.emplace_back(std::move(list));
        }

        rowCount++;
        if (iter->valid()) {
            resp_.set_has_next(true);
            resp_.set_next_cursor(iter->key().str());
        } else {
            resp_.set_has_next(false);
        }
        onProcessFinished();
        onFinished();
    }
}

cpp2::ErrorCode ScanProcessor::checkAndBuildContexts(const cpp2::ScanRequest& /*req*/) {
    return cpp2::ErrorCode::SUCCEEDED;
}

void ScanProcessor::buildColumnName(const std::vector<cpp2::SchemaProp>& properties) {
    for (const auto& property : properties) {
        if (isEdge_) {
            auto edgeType = property.get_schema().get_edge_type();
            auto edgeName = edgeContext_.edgeNames_[edgeType];
            for (const auto& prop : property.get_props()) {
                resultDataSet_.colNames.emplace_back(edgeName + "." + prop);
            }
        } else {
            auto tagId = property.get_schema().get_tag_id();
            auto tagName = tagContext_.tagNames_[tagId];
            for (const auto& prop : property.get_props()) {
                resultDataSet_.colNames.emplace_back(tagName + "." + prop);
            }
        }
    }
}

void ScanProcessor::onProcessFinished() {
    resp_.set_vertex_data(std::move(resultDataSet_));
}

}  // namespace storage
}  // namespace nebula
