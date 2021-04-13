/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "utils/NebulaKeyUtils.h"
#include "kvstore/plugins/kafka/KafkaListener.h"

DEFINE_string(kafka_topic_name, "", "Kafka topic's name");
DEFINE_string(kafka_brokers, "127.0.0.1:9092", "Kafka brokers' addresses");

namespace nebula {
namespace kvstore {

bool KafkaListener::init() {
    auto vRet = schemaMan_->getSpaceVidLen(spaceId_);
    if (!vRet.ok()) {
        LOG(ERROR) << "vid length error";
        return false;
    }

    vIdLen_ = vRet.value();
    client_ = std::make_unique<nebula::kafka::KafkaClient>(FLAGS_kafka_brokers);
    return true;
}

bool KafkaListener::apply(const std::vector<KV>& data) {
    for (const auto& kv : data) {
        if (nebula::NebulaKeyUtils::isVertex(vIdLen_, kv.first)) {
            if (!appendVertex(kv)) {
                LOG(ERROR) << "Append vertex failed";
                return false;
            }
        } else if (nebula::NebulaKeyUtils::isEdge(vIdLen_, kv.first)) {
            if (!appendEdge(kv)) {
                LOG(ERROR) << "Append edge failed";
                return false;
            }
        } else {
            VLOG(3) << "Not vertex or edge data. Skip";
        }
    }
    return true;
}

bool KafkaListener::appendVertex(const KV& kv) const {
    auto tagId = NebulaKeyUtils::getTagId(vIdLen_, kv.first);
    auto part = NebulaKeyUtils::getPart(kv.first);
    auto reader = RowReaderWrapper::getTagPropReader(schemaMan_,
                                                     spaceId_,
                                                     tagId,
                                                     kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get tag reader failed, tagID " << tagId;
        return false;
    }
    return appendMessage(part, reader.get());
}

bool KafkaListener::appendEdge(const KV& kv) const {
    auto edgeType = NebulaKeyUtils::getEdgeType(vIdLen_, kv.first);
    auto part = NebulaKeyUtils::getPart(kv.first);
    auto reader = RowReaderWrapper::getEdgePropReader(schemaMan_,
                                                      spaceId_,
                                                      edgeType,
                                                      kv.second);
    if (reader == nullptr) {
        VLOG(3) << "get edge reader failed, schema ID " << edgeType;
        return false;
    }
    return appendMessage(part, reader.get());
}

bool KafkaListener::appendMessage(PartitionID part, RowReader* reader) const {
    LOG(INFO) << "Send Message to Kafka";
    int32_t size = reader->numFields();
    folly::dynamic map = folly::dynamic::object;
    auto schema = reader->getSchema();
    for (int32_t index = 0; index < size; index++) {
        auto key = schema->getFieldName(index);
        auto value = reader->getValueByIndex(index);
        folly::dynamic dynamicValue;
        if (value.isStr()) {
            dynamicValue = value.getStr();
        } else if (value.isInt()) {
            dynamicValue = value.getInt();
        } else if (value.isFloat()) {
            dynamicValue = value.getFloat();
        } else if (value.isBool()) {
            dynamicValue = value.getBool();
        }
        map[key] = dynamicValue;
    }

    auto value = folly::toJson(map);
    LOG(INFO) << "JSON: " << value;

    // TODO(darion) should make sure the kafka part size same with nebula graph
    client_->produce(FLAGS_kafka_topic_name, part, "", std::move(value));
    return true;
}

}  // namespace kvstore
}  // namespace nebula
