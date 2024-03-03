#pragma once

#include <optional>

namespace fpindex {
namespace util {

template <typename Callback = void()>
class Cleanup {
 public:
    Cleanup(Callback callback) : callback_(std::move(callback)) {}
    ~Cleanup() { Invoke(); }

    void Cancel() { callback_.reset(); }
    void Invoke() {
        if (callback_) {
            auto cb = std::move(*callback_);
            callback_.reset();
            cb();
        }
    }

 private:
    std::optional<Callback> callback_;
};

template <typename Callback>
Cleanup<Callback> MakeCleanup(Callback callback) {
    return {std::move(callback)};
}

}  // namespace util
}  // namespace fpindex
