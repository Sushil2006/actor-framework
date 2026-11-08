// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/main/LICENSE.

#include "caf/test/test.hpp"

#include "caf/actor_from_state.hpp"
#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/anon_mail.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/telemetry/metric_registry.hpp"

#include <barrier>
#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

using namespace caf;
using namespace std::chrono_literals;

namespace {

struct race_probe_state {
  static constexpr const char* name = "metrics-race-probe";

  explicit race_probe_state(stateful_actor<race_probe_state>* ptr,
                            std::barrier<>& gate, std::jthread& sender)
    : self(ptr) {
    sender = std::jthread([ptr, &gate] {
      gate.arrive_and_wait();
      anon_mail(42).send(ptr);
    });
    // Both enqueue and setup_metrics proceed after this barrier, unordered.
    gate.arrive_and_wait();
  }

  behavior make_behavior() {
    return {
      [this](int) { self->quit(); },
    };
  }

  stateful_actor<race_probe_state>* self;
};

} // namespace

// ThreadSanitizer checks pointer access; without it, this only checks delivery.
TEST("actor metrics setup is safe during concurrent enqueue") {
  actor_system_config cfg;
  cfg.set("caf.scheduler.max-threads", 1);
  cfg.set("caf.metrics.filters.actors.includes",
          std::vector<std::string>{race_probe_state::name});
  actor_system sys{cfg};
  std::barrier gate{2};
  std::jthread sender;

  auto hdl = sys.spawn(actor_from_state<race_probe_state>, gate, sender);
  // Joining inside the constructor would order enqueue before metrics setup.
  sender.join();
  require(sys.metrics().wait_for("caf.actor", "processed-messages",
                                {{"name", race_probe_state::name}}, 5s, 1ms,
                                [](int64_t value) { return value == 1; }));
  sys.await_all_actors_done();
}
