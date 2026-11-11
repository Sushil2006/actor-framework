// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/main/LICENSE.

#include "caf/test/test.hpp"

#include "caf/actor_from_state.hpp"
#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/anon_mail.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/exit_reason.hpp"
#include "caf/send.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/telemetry/counter.hpp"

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace caf;
using namespace std::chrono_literals;

namespace {

// Single-use two-party barrier (std::barrier is C++20).
class gate {
public:
  void arrive_and_wait() {
    std::unique_lock guard{mtx_};
    if (++count_ == 2) {
      cv_.notify_all();
      return;
    }
    cv_.wait(guard, [this] { return count_ == 2; });
  }

private:
  std::mutex mtx_;
  std::condition_variable cv_;
  int count_ = 0;
};

struct race_probe_state {
  static constexpr const char* name = "metrics-race-probe";

  race_probe_state(stateful_actor<race_probe_state>* ptr, gate& g,
                   std::thread& sender)
    : self(ptr) {
    sender = std::thread([ptr, &g] {
      g.arrive_and_wait();
      anon_mail(42).send(actor{ptr});
    });
    // Both enqueue and setup_metrics proceed after this point, unordered.
    g.arrive_and_wait();
  }

  behavior make_behavior() {
    return {
      // Don't quit here: resume() only counts successfully handled messages.
      [](int) {},
    };
  }

  stateful_actor<race_probe_state>* self;
};

template <class Predicate>
bool wait_until(Predicate pred) {
  for (int i = 0; i < 5000 && !pred(); ++i)
    std::this_thread::sleep_for(1ms);
  return pred();
}

} // namespace

// Run under a race detector (TSan or Helgrind) to check the unsynchronized
// metrics access; on its own, this test only checks message delivery.
TEST("actor metrics setup is safe during concurrent enqueue") {
  actor_system_config cfg;
  cfg.set("caf.scheduler.max-threads", 1);
  cfg.set("caf.metrics-filters.actors.includes",
          std::vector<std::string>{race_probe_state::name});
  actor_system sys{cfg};
  auto* processed = sys.base_metrics().processed_messages;
  auto baseline = processed->value();
  gate g;
  std::thread sender;
  auto hdl = sys.spawn(actor_from_state<race_probe_state>, std::ref(g),
                       std::ref(sender));
  // Joining inside the constructor would order enqueue before metrics setup.
  sender.join();
  require(wait_until([&] { return processed->value() > baseline; }));
  anon_send_exit(hdl, exit_reason::user_shutdown);
}
