#pragma once

#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "iceberg/common/error.h"

namespace iceberg {

/// A type erased callable object which may only be invoked once.
/// It can be constructed from any lambda which matches the provided call signature.
/// Invoking it results in destruction of the lambda, freeing any state/references
/// immediately. Invoking a default constructed FnOnce or one which has already been
/// invoked will segfault.
template <typename Signature>
class FnOnce;

template <typename R, typename... A>
class FnOnce<R(A...)> {
 public:
  FnOnce() = default;

  template <typename Fn, typename = typename std::enable_if<
                             std::is_convertible<decltype(std::declval<Fn&&>()(std::declval<A>()...)), R>::value>::type>
  FnOnce(Fn fn) : impl_(new FnImpl<Fn>(std::move(fn))) {  // NOLINT runtime/explicit
  }

  explicit operator bool() const { return impl_ != nullptr; }

  R operator()(A... a) && {
    auto bye = std::move(impl_);
    return bye->invoke(std::forward<A&&>(a)...);
  }

 private:
  struct Impl {
    virtual ~Impl() = default;
    virtual R invoke(A&&... a) = 0;
  };

  template <typename Fn>
  struct FnImpl : Impl {
    explicit FnImpl(Fn fn) : fn_(std::move(fn)) {}
    R invoke(A&&... a) override { return std::move(fn_)(std::forward<A&&>(a)...); }
    Fn fn_;
  };

  std::unique_ptr<Impl> impl_;
};

class ThreadPool {
 public:
  explicit ThreadPool(size_t threads_count) {
    threads_.reserve(threads_count);
    for (size_t i = 0; i < threads_count; ++i) {
      threads_.emplace_back([this]() { this->RunLoop(); });
    }
  }

  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;
  ThreadPool(ThreadPool&&) = delete;
  ThreadPool& operator=(ThreadPool&&) = delete;

  template <typename Foo>
  std::future<std::invoke_result_t<Foo>> Submit(Foo func) {
    std::lock_guard lg(mutex_);
    Ensure(status == Status::kRunning, "Submit is impossible, thread pool is stopped");

    std::packaged_task<std::invoke_result_t<Foo>()> task(std::move(func));
    auto future = task.get_future();

    tasks_to_do_.emplace(std::move(task));
    has_work_.notify_one();

    return future;
  }

  template <typename Foo>
  std::invoke_result_t<Foo> Invoke(Foo func) {
    auto future = Submit(std::move(func));
    return future.get();
  }

  void Stop(bool wait_for_remaining_tasks = true) {
    {
      std::lock_guard lg(mutex_);
      if (status != Status::kRunning) {
        return;
      }
      if (wait_for_remaining_tasks) {
        status = Status::kWaitPending;
      } else {
        status = Status::kCancelPending;
      }

      has_work_.notify_all();
    }

    bool has_exception = false;
    for (auto& thread : threads_) {
      try {
        thread.join();
      } catch (...) {
        has_exception = true;
      }
    }

    Ensure(!has_exception, "ThreadPool::Stop() failed");
  }

  ~ThreadPool() {
    try {
      Stop();
    } catch (...) {
    }
  }

 private:
  using Task = FnOnce<void()>;

  void WaitUntilStoppedOrHasTask(std::unique_lock<std::mutex>& lg) {
    while (tasks_to_do_.empty() && status == Status::kRunning) {
      has_work_.wait(lg);
    }
  }

  Task GetTask() {
    std::unique_lock lg(mutex_);
    WaitUntilStoppedOrHasTask(lg);

    if (status == Status::kWaitPending && tasks_to_do_.empty()) {
      status = Status::kCancelPending;
    }

    if (status == Status::kCancelPending) {
      return {};
    }

    auto result = std::move(tasks_to_do_.front());
    tasks_to_do_.pop();
    return result;
  }

  void RunLoop() {
    while (auto task = GetTask()) {
      std::move(task)();
    }
  }

  enum class Status { kRunning, kWaitPending, kCancelPending };

  Status status = Status::kRunning;
  std::queue<Task> tasks_to_do_;
  std::condition_variable has_work_;
  std::mutex mutex_;

  std::vector<std::thread> threads_;
};

}  // namespace iceberg
