#pragma once

namespace fpindex {

class IndexUpdate {
 public:
    IndexUpdate() = default;

    void InsertOrUpdate(uint32_t id, const std::vector<uint64_t>& hashes) {
        auto& entry = entries_.emplace_back();
        auto request = entry.mutable_data()->mutable_insert_or_update();
        request->set_id(id);
        request->mutable_hashes()->Add(hashes.begin(), hashes.end());
    }

    void Delete(uint32_t id) {
        auto& entry = entries_.emplace_back();
        auto request = entry.mutable_data()->mutable_delete_();
        request->set_id(id);
    }

    void SetAttribute(const std::string& name, const std::string& value) {
        auto& entry = entries_.emplace_back();
        auto request = entry.mutable_data()->mutable_set_attribute();
        request->set_name(name);
        request->set_value(value);
    }

    std::vector<OplogEntry> Finish() { return std::move(entries_); }

 private:
    std::vector<OplogEntry> entries_;
};

}  // namespace fpindex
