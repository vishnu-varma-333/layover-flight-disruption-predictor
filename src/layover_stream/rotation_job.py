"""The real Flink job: event-time watermarks, keyed state, explicit
duplicate/late handling -- replacing Milestone 4's Python, producer-
sorted-order assumption with genuine stream processing that tolerates
whatever order events actually arrive in.

Per-tail state (last-landed-leg info, pending not-yet-departed flights,
a leg counter, a seen-keys set for dedup) lives in real Flink state
(ValueState/MapState, JSON-serialized), scoped per key by `keyBy
(tail_number)` -- Flink partitions and checkpoints this automatically.
The actual state-transition *logic* is delegated to the pure functions in
`rotation_state.py`, kept separately testable without a Flink runtime.

Two things handled explicitly, both via Flink's own watermark mechanism,
not ad hoc timestamp comparisons:
- **Late** events (arrived more than `ALLOWED_LATENESS_SECONDS` of event
  time behind the current watermark) are routed to a side output, not
  silently processed as if they were still timely.
- **Duplicate** events (same flight_id + event_type seen before) are
  dropped before they can touch state at all, via an explicit seen-keys
  MapState -- not relied on being naturally idempotent.
"""

import json
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from pyflink.common import Duration, SimpleStringSchema, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import (
    KeyedProcessFunction,
    RuntimeExecutionMode,
    StreamExecutionEnvironment,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaRecordSerializationSchema,
    KafkaSink,
    KafkaSource,
)
from pyflink.datastream.functions import RuntimeContext
from pyflink.datastream.output_tag import OutputTag
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from layover_data.airports import build_airport_tz_map
from layover_stream.rotation_state import build_feature_update, merge_landed_event, resolve_inbound

AIRPORTS_CSV = (
    Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "reference" / "airports.csv"
)

CONNECTOR_JAR = (
    "file:///Users/chanakya/.local/flink-connectors/flink-sql-connector-kafka-5.0.0-2.2.jar"
)
BOOTSTRAP_SERVERS = "localhost:19092"
INPUT_TOPIC = "stream.flight_events"
FEATURES_TOPIC = "stream.flight_features"
LATE_TOPIC = "stream.late_events"

ALLOWED_LATENESS_SECONDS = 300
BOUNDED_OUT_OF_ORDERNESS_SECONDS = 60

LATE_EVENTS_TAG = OutputTag("late-events", Types.STRING())


class EventTimeAssigner(TimestampAssigner):
    """Events carry `event_time` as Unix seconds (float/int) -- Flink
    timestamps are always epoch milliseconds."""

    def extract_timestamp(self, value, record_timestamp):
        event = json.loads(value)
        return int(event["event_time"] * 1000)


class RotationLinkFunction(KeyedProcessFunction):
    def open(self, runtime_context: RuntimeContext):
        self.tail_state = runtime_context.get_state(
            ValueStateDescriptor("tail_state", Types.STRING())
        )
        self.pending = runtime_context.get_map_state(
            MapStateDescriptor("pending", Types.STRING(), Types.STRING())
        )
        self.leg_index = runtime_context.get_map_state(
            MapStateDescriptor("leg_index", Types.STRING(), Types.INT())
        )
        self.seen_keys = runtime_context.get_map_state(
            MapStateDescriptor("seen_keys", Types.STRING(), Types.BOOLEAN())
        )
        # Loaded once per task, not per event -- same pattern as every other
        # script that needs this lookup (train_model.py, run_backtest.py).
        self.airport_tz_map = build_airport_tz_map(AIRPORTS_CSV)

    def _full_feature_update(
        self, flight: dict, leg_index: int, inbound, source_event_type, event_time
    ):
        """Builds the complete feature record the models were trained on
        (layover_model.features.SCHEDULE_FEATURES + ROTATION_FEATURES):
        the pure rotation-state fields from `build_feature_update`, plus
        `leg_index`/`chain_break` and the schedule-side context
        (carrier/origin/dest/month/day_of_week/dep_hour_local) that
        function deliberately doesn't compute.

        `sched_dep_utc` only survives the Kafka wire as a string (JSON has
        no timestamp type) -- parsed back into a tz-aware Timestamp once
        here and reused for both the local-time parts and (as epoch
        seconds) `turnaround_minutes`, so there's exactly one parse and
        one source of truth for "this flight's scheduled departure"
        rather than two call sites quietly assuming the same value.

        Found during Milestone 7's research pass, two real gaps, both
        fixed here (see DECISIONS.md for the full writeup):
        1. This job's feature output originally carried only the
           rotation-state subset (verify_rotation_job.py's own old
           FEATURE_FIELDS list) -- silently missing every field below.
        2. `build_feature_update` used to measure `turnaround_minutes`
           from the *triggering event's own* `event_time`, not the
           flight's scheduled departure -- correct only for a
           coincidence of units, wrong by ~24h at schedule time and by
           this flight's own delay/duration at departed/landed time.
           Training's actual definition is always
           `sched_dep_utc - prev_actual_arr_utc`, independent of which
           event is doing the recomputing.
        """
        raw_sched_dep = flight.get("sched_dep_utc")
        sched_dep_ts = pd.Timestamp(raw_sched_dep) if raw_sched_dep else None
        sched_dep_epoch = sched_dep_ts.timestamp() if sched_dep_ts is not None else None

        features = build_feature_update(
            flight["flight_id"],
            flight["tail_number"],
            flight["fl_date"],
            inbound,
            source_event_type,
            event_time,
            sched_dep_epoch=sched_dep_epoch,
        )

        tz = self.airport_tz_map.get(flight["origin"])
        if tz is not None and sched_dep_ts is not None:
            local = sched_dep_ts.to_pydatetime().astimezone(ZoneInfo(tz))
            dep_hour, dow, month = local.hour, local.weekday(), local.month
        else:
            dep_hour, dow, month = None, None, None

        features["leg_index"] = leg_index
        features["chain_break"] = (inbound["dest"] != flight["origin"]) if inbound else False
        features["carrier"] = flight["carrier"]
        features["origin"] = flight["origin"]
        features["dest"] = flight["dest"]
        features["dep_hour_local"] = dep_hour
        features["day_of_week"] = dow
        features["month"] = month
        return features

    def process_element(self, value, ctx: "KeyedProcessFunction.Context"):
        watermark = ctx.timer_service().current_watermark()
        if watermark is not None and watermark > -(2**63) and ctx.timestamp() is not None:
            if ctx.timestamp() < watermark - ALLOWED_LATENESS_SECONDS * 1000:
                # PyFlink's Python API has no ctx.output() (unlike Java) --
                # a side output is emitted by yielding (tag, value) from
                # process_element itself. Found live: ctx.output() raised
                # AttributeError the first time a genuinely late event
                # reached this branch. See DECISIONS.md.
                yield (LATE_EVENTS_TAG, value)
                return

        event = json.loads(value)
        dedup_key = f"{event['flight_id']}:{event['event_type']}"
        if self.seen_keys.contains(dedup_key):
            return
        self.seen_keys.put(dedup_key, True)

        event_time = event["event_time"]
        etype = event["event_type"]
        tail = event["tail_number"]
        fl_date = event["fl_date"]

        raw_tail_state = self.tail_state.value()
        current_tail_state = json.loads(raw_tail_state) if raw_tail_state else None

        if etype == "schedule":
            leg_key = f"{tail}:{fl_date}"
            idx = self.leg_index.get(leg_key)
            idx = 0 if idx is None else idx
            self.leg_index.put(leg_key, idx + 1)

            inbound = resolve_inbound(current_tail_state, fl_date)
            features = self._full_feature_update(event, idx, inbound, "schedule", event_time)
            yield json.dumps(features)

            self.pending.put(event["flight_id"], json.dumps({**event, "leg_index": idx}))

        elif etype == "departed":
            pending_json = self.pending.get(event["flight_id"])
            if pending_json is None:
                return
            self.pending.remove(event["flight_id"])
            pending_entry = json.loads(pending_json)
            inbound = resolve_inbound(current_tail_state, fl_date)
            features = self._full_feature_update(
                pending_entry, pending_entry["leg_index"], inbound, "departed", event_time
            )
            features["dep_delay_min"] = event["dep_delay_min"]
            yield json.dumps(features)

        elif etype == "landed":
            new_tail_state = merge_landed_event(current_tail_state, event)
            self.tail_state.update(json.dumps(new_tail_state))

            # Already inside a keyBy(tail_number) partition -- every entry
            # in `pending` here already belongs to this tail, so there's
            # no need to filter by tail_number, only by fl_date.
            for flight_id in list(self.pending.keys()):
                pending_entry = json.loads(self.pending.get(flight_id))
                if pending_entry["fl_date"] != fl_date:
                    continue
                inbound = resolve_inbound(new_tail_state, fl_date)
                features = self._full_feature_update(
                    pending_entry, pending_entry["leg_index"], inbound, "landed", event_time
                )
                yield json.dumps(features)


def build_job(env: StreamExecutionEnvironment, bounded: bool = False):
    env.add_jars(CONNECTOR_JAR)

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP_SERVERS)
        .set_topics(INPUT_TOPIC)
        .set_group_id("rotation-link-job")
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if bounded:
        # Verification convenience: stop once caught up to the current end
        # offset at job start, instead of running as an unbounded stream
        # forever. Real deployment uses the unbounded default.
        source_builder = source_builder.set_bounded(KafkaOffsetsInitializer.latest())
    source = source_builder.build()
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(BOUNDED_OUT_OF_ORDERNESS_SECONDS)
    ).with_timestamp_assigner(EventTimeAssigner())

    # Found live: binding the watermark strategy directly via
    # from_source(source, watermark_strategy, ...) does NOT propagate the
    # assigned event-time to ctx.timestamp() inside a Python
    # KeyedProcessFunction reading from a KafkaSource -- ctx.timestamp()
    # silently fell back to Kafka's own record timestamp (effectively
    # produce wall-clock time) instead. Confirmed by a minimal isolated
    # test: the same EventTimeAssigner attached via the explicit two-step
    # `from_source(..., WatermarkStrategy.no_watermarks(), ...)` then
    # `.assign_timestamps_and_watermarks(...)` on the resulting stream
    # DOES propagate correctly. See DECISIONS.md.
    stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "flight-events")
    stream = stream.assign_timestamps_and_watermarks(watermark_strategy)
    keyed = stream.key_by(lambda v: json.loads(v)["tail_number"])
    processed = keyed.process(RotationLinkFunction(), output_type=Types.STRING())
    late_stream = processed.get_side_output(LATE_EVENTS_TAG)

    def _sink(topic):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(BOOTSTRAP_SERVERS)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

    processed.sink_to(_sink(FEATURES_TOPIC))
    late_stream.sink_to(_sink(LATE_TOPIC))
    return env


def _reset_output_topics():
    """Bounded (verification) runs only: delete and recreate the output
    topics before running, the same way produce_flight_events.py resets
    the input topic.

    Found live, during Milestone 7: neither this job nor
    produce_flight_events.py ever reset `stream.flight_features` /
    `stream.late_events`, so every repeated verification run's output
    kept accumulating in them rather than replacing the previous run's --
    a fresh, uniquely-named consumer group still reads every prior run's
    messages too (`auto.offset.reset: earliest`), inflating counts and
    occasionally resolving "last per flight" to a stale run. See
    DECISIONS.md.
    """
    import time as _time

    from confluent_kafka.admin import AdminClient

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for f in admin.delete_topics([FEATURES_TOPIC, LATE_TOPIC]).values():
        try:
            f.result()
        except Exception:  # noqa: BLE001 -- "topic doesn't exist yet" is fine
            pass
    _time.sleep(1)


def main():
    bounded = "--bounded" in sys.argv
    if bounded:
        _reset_output_topics()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    build_job(env, bounded=bounded)
    env.execute("rotation-link-job")


if __name__ == "__main__":
    main()
