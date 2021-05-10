/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/IngestTask.h"

namespace nebula {
namespace storage {

bool IngestTask::check() {
    return env_->kvstore_ != nullptr;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
IngestTask::genSubTasks() {
    std::vector<AdminSubTask> ret;
    auto* store = dynamic_cast<kvstore::NebulaStore*>(env_->kvstore_);
    auto errOrSpace = store->space(*ctx_.parameters_.space_id_ref());
    if (!ok(errOrSpace)) {
        LOG(ERROR) << "Space not found";
        return error(errOrSpace);
    }

    auto space = nebula::value(errOrSpace);
    ret.emplace_back([space = space]() {
        for (auto& engine : space->engines_) {
            std::vector<std::string> files;
            auto code = engine->ingest(files);
            if (code != nebula::cpp2::ErrorCode::SUCCEEDED) {
                return code;
            }
        }
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    });
    return ret;
}

}  // namespace storage
}  // namespace nebula
