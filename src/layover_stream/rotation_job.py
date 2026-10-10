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

from layover_stream.rotation_state import build_feature_update, merge_landed_event, resolve_inbound

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
            features = build_feature_update(
                event["flight_id"], tail, fl_date, inbound, "schedule", event_time
            )
            yield json.dumps(features)

            self.pending.put(event["flight_id"], json.dumps({**event, "leg_index": idx}))

        elif etype == "departed":
            pending_json = self.pending.get(event["flight_id"])
            if pending_json is None:
                return
            self.pending.remove(event["flight_id"])
            inbound = resolve_inbound(current_tail_state, fl_date)
            features = build_feature_update(
                event["flight_id"], tail, fl_date, inbound, "departed", event_time
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
                features = build_feature_update(
                    flight_id, tail, fl_date, inbound, "landed", event_time
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


def main():
    bounded = "--bounded" in sys.argv
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)
    build_job(env, bounded=bounded)
    env.execute("rotation-link-job")


if __name__ == "__main__":
    main()
