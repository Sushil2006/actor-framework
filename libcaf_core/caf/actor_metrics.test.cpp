// This file is part of CAF, the C++ Actor Framework. See the file LICENSE in
// the main distribution directory for license terms and copyright or visit
// https://github.com/actor-framework/actor-framework/blob/main/LICENSE.

#include "caf/test/test.hpp"

#include "caf/actor_from_state.hpp"
#include "caf/actor_system.hpp"
#include "caf/actor_system_config.hpp"
#include "caf/event_based_actor.hpp"
#include "caf/exit_reason.hpp"
#include "caf/send.hpp"
#include "caf/stateful_actor.hpp"
#include "caf/telemetry/counter.hpp"
#include "caf/telemetry/histogram.hpp"
#include "caf/telemetry/int_gauge.hpp"
#include "caf/telemetry/metric_family_impl.hpp"

#include <chrono>
#include <cstdint>
#include <string>
#include <thread>
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
      // Don't quit here: resume() only counts successfully handled messages.
      [](int) {},
    };
  }

  stateful_actor<probe_state>* self;
};

template <class Predicate>
bool wait_until(Predicate pred) {
  for (int i = 0; i < 5000 && !pred(); ++i)
    std::this_thread::sleep_for(1ms);
  return pred();
}

} // namespace

TEST("actor metrics account for messages sent during state construction") {
  actor_system_config cfg;
  cfg.set("caf.scheduler.max-threads", 1);
  cfg.set("caf.metrics-filters.actors.includes",
          std::vector<std::string>{probe_state::name});
  actor_system sys{cfg};
  auto* processed = sys.base_metrics().processed_messages;
  auto baseline = processed->value();
  auto start = std::chrono::steady_clock::now();
  auto hdl = sys.spawn(actor_from_state<probe_state>);
  // resume() bumps this counter only after run_with_metrics has updated the
  // mailbox metrics for the message.
  require(wait_until([&] { return processed->value() > baseline; }));
  auto end = std::chrono::steady_clock::now();
  const auto& families = sys.actor_metric_families();
  require_ne(families.mailbox_size, nullptr);
  require_ne(families.mailbox_time, nullptr);
  auto* mailbox_size
    = families.mailbox_size->get_or_add({{"name", probe_state::name}});
  auto* mailbox_time
    = families.mailbox_time->get_or_add({{"name", probe_state::name}});

  check_eq(mailbox_size->value(), 0);

  int64_t observations = 0;
  for (const auto& bucket : mailbox_time->buckets())
    observations += bucket.count.value();
  check_eq(observations, 1);
  auto max_wait = std::chrono::duration<double>(end - start).count();
  check_ge(mailbox_time->sum(), 0.0);
  check_le(mailbox_time->sum(), max_wait);
  anon_send_exit(hdl, exit_reason::user_shutdown);
}
