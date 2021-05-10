/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

#include "storage/admin/DownloadTask.h"

namespace nebula {
namespace storage {

bool DownloadTask::check() {
    return env_->kvstore_ != nullptr;
}

ErrorOr<nebula::cpp2::ErrorCode, std::vector<AdminSubTask>>
DownloadTask::genSubTasks() {
    auto space = *ctx_.parameters_.space_id_ref();
    auto parts = *ctx_.parameters_.parts_ref();
    auto paras = ctx_.parameters_.task_specfic_paras_ref();
    if (!paras.has_value() || paras->empty()) {
        LOG(ERROR) << "Download Task should not empty";
        return nebula::cpp2::ErrorCode::E_INVALID_PARM;
    }

    std::vector<AdminSubTask> tasks;
    for (const auto& part : parts) {
        TaskFunction task = std::bind(&DownloadTask::invoke, this, space, part);
        tasks.emplace_back(std::move(task));
    }
    return tasks;
}

nebula::cpp2::ErrorCode
DownloadTask::invoke(GraphSpaceID space, PartitionID part) {
    LOG(INFO) << "Space: " << space << " Part: " << part;
    auto hdfsPartPath = folly::stringPrintf("%s/%d", hdfsPath_.c_str(), part);
    auto partResult = env_->kvstore_->part(space, part);
    if (!ok(partResult)) {
        LOG(ERROR) << "Can't found space: " << space << ", part: " << part;
        return nebula::cpp2::ErrorCode::E_PART_NOT_FOUND;
    }

    auto localPath = folly::stringPrintf("%s/download/",
                                         value(partResult)->engine()->getDataRoot());
    auto result = this->helper_->copyToLocal(hdfsHost_, hdfsPort_,
                                             hdfsPartPath, localPath);

    if (result.ok() && result.value().empty()) {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    } else {
        return nebula::cpp2::ErrorCode::SUCCEEDED;
    }
}

}  // namespace storage
}  // namespace nebula
