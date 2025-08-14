#include "iceberg/common/threadpool.h"

#include <cstdint>
#include <stdexcept>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

namespace iceberg {
namespace {

TEST(ThreadPool, Simple) {
  for (int j = 0; j < 1'000; ++j) {
    ThreadPool thread_pool(1);

    int variable = 0;
    for (int i = 0; i < 100; ++i) {
      thread_pool.Invoke([&variable]() { variable++; });
      EXPECT_EQ(variable, i + 1);
    }

    thread_pool.Stop();
  }
}

TEST(ThreadPool, ReturnsValue) {
  ThreadPool thread_pool(1);
  auto future = thread_pool.Submit([]() { return 1; });

  EXPECT_EQ(future.get(), 1);
}

TEST(ThreadPool, Exception) {
  ThreadPool thread_pool(1);

  auto fut = thread_pool.Submit([]() { throw std::runtime_error("X"); });
  EXPECT_ANY_THROW(fut.get());

  thread_pool.Stop();
}

TEST(ThreadPool, ExceptionNonVoidType) {
  ThreadPool thread_pool(1);

  int value = 2;
  auto fut = thread_pool.Submit([&]() {
    if (value == 2) {
      throw std::runtime_error("X");
    }
    return 1;
  });
  EXPECT_ANY_THROW(fut.get());

  thread_pool.Stop();
}

TEST(ThreadPool, Stop) {
  for (int j = 0; j < 1'000; ++j) {
    ThreadPool thread_pool(1);

    int variable = 0;
    for (int i = 0; i < 5; ++i) {
      auto wait = thread_pool.Submit([&variable]() { variable++; });
      wait.get();
      EXPECT_EQ(variable, i + 1);
    }

    thread_pool.Stop();
  }
}

TEST(ThreadPool, Async) {
  for (int j = 0; j < 1'000; ++j) {
    ThreadPool thread_pool(1);
    std::atomic<int> variable = 0;

    auto future = thread_pool.Submit([&variable]() {
      while (variable.load() == 0) {
      }
      variable.store(2);
    });

    EXPECT_EQ(variable.load(), 0);
    variable.store(1);
    while (variable.load() == 1) {
    }
    EXPECT_EQ(variable.load(), 2);
  }
}

TEST(ThreadPool, ManyThreads) {
  for (int j = 0; j < 1'000; ++j) {
    constexpr int32_t kThreads = 5;

    ThreadPool thread_pool(kThreads);
    std::atomic<int> variable = 0;
    std::atomic<int> done = 0;

    std::vector<std::future<void>> futures;
    for (int i = 0; i < kThreads - 1; ++i) {
      futures.emplace_back(thread_pool.Submit([&variable, &done]() {
        while (variable.load() == 0) {
        }
        done.fetch_add(1);
      }));
    }

    int other_variable = 0;
    for (int i = 0; i < 100; ++i) {
      thread_pool.Invoke([&other_variable]() { other_variable++; });
      EXPECT_EQ(other_variable, i + 1);
    }

    variable.store(1);

    thread_pool.Stop();
    EXPECT_EQ(done.load(), kThreads - 1);
  }
}

TEST(ThreadPool, ManyTasks) {
  for (int j = 0; j < 3; ++j) {
    constexpr int32_t kThreads = 5;
    constexpr int32_t kTasksToDo = 100'000;

    ThreadPool thread_pool(kThreads);
    std::atomic<uint32_t> tasks_started = 0;
    std::atomic<uint32_t> tasks_done = 0;

    auto task = [&]() {
      tasks_started.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      tasks_done.fetch_add(1);
    };

    std::vector<std::future<void>> futures;
    futures.reserve(kTasksToDo);

    for (int32_t i = 0; i < kTasksToDo; ++i) {
      futures.emplace_back(thread_pool.Submit(task));
    }

    thread_pool.Stop();

    ASSERT_EQ(tasks_done.load(), kTasksToDo);
    ASSERT_EQ(tasks_started.load(), kTasksToDo);
  }
}

TEST(ThreadPool, IgnoreStoppedTasks) {
  for (int j = 0; j < 3; ++j) {
    constexpr int32_t kThreads = 5;
    constexpr int32_t kTasksToDo = 100'000;

    ThreadPool thread_pool(kThreads);
    std::atomic<uint32_t> tasks_started = 0;
    std::atomic<uint32_t> tasks_done = 0;

    std::atomic<bool> continue_blocked_task{false};
    auto blocked_task = [&]() {
      while (!continue_blocked_task.load()) {
      }
    };

    auto task = [&]() {
      tasks_started.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::microseconds(1));
      tasks_done.fetch_add(1);
    };

    std::vector<std::future<void>> futures;
    futures.reserve(kTasksToDo + kThreads);

    for (int32_t i = 0; i < kThreads; ++i) {
      futures.emplace_back(thread_pool.Submit(blocked_task));
    }

    for (int32_t i = 0; i < kTasksToDo; ++i) {
      futures.emplace_back(thread_pool.Submit(task));
    }

    continue_blocked_task.store(true);
    thread_pool.Stop(false);

    ASSERT_EQ(tasks_done.load(), tasks_started.load());
    ASSERT_LE(tasks_done.load(), 1000);
  }
}

}  // namespace
}  // namespace iceberg
