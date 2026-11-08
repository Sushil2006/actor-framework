// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/main/LICENSE.

#include "caf/telemetry/actor_metrics.hpp"

#include "caf/test/test.hpp"

#include "caf/actor_from_state.hpp"
#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/telemetry/counter.hpp"
#include "caf/telemetry/gauge.hpp"
#include "caf/telemetry/histogram.hpp"
#include "caf/telemetry/metric_registry.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

using namespace caf;
using namespace std::chrono_literals;

namespace {

struct probe_state {
  static constexpr const char* name = "metrics-probe";

  explicit probe_state(stateful_actor<probe_state>* ptr) : self(ptr) {
    // Enqueue before make_actor calls setup_metrics, without a timing race.
    self->mail(42).send(self);
  }

  behavior make_behavior() {
    return {
      [this](int) { self->quit(); },
    };
  }

  stateful_actor<probe_state>* self;
};

} // namespace

TEST("actor metrics account for messages sent during state construction") {
  actor_system_config cfg;
  cfg.set("caf.scheduler.max-threads", 1);
  cfg.set("caf.metrics.filters.actors.includes",
          std::vector<std::string>{probe_state::name});
  actor_system sys{cfg};

  auto start = std::chrono::steady_clock::now();
  auto hdl = sys.spawn(actor_from_state<probe_state>);
  sys.await_all_actors_done();
  // Termination can be signaled before consume finishes updating the metrics.
  require(sys.metrics().wait_for("caf.actor", "processed-messages",
                                {{"name", probe_state::name}}, 5s, 1ms,
                                [](int64_t value) { return value == 1; }));
  auto end = std::chrono::steady_clock::now();
  auto metrics = sys.make_actor_metrics(probe_state::name);
  require_ne(metrics.mailbox_size, nullptr);
  require_ne(metrics.mailbox_time, nullptr);

  check_eq(metrics.mailbox_size->value(), 0);

  int64_t observations = 0;
  for (const auto& bucket : metrics.mailbox_time->buckets())
    observations += bucket.count.value();
  check_eq(observations, 1);
  auto max_wait = std::chrono::duration<double>(end - start).count();
  check_ge(metrics.mailbox_time->sum(), 0.0);
  check_le(metrics.mailbox_time->sum(), max_wait);
}
