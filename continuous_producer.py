#!/usr/bin/env python3
"""
CT Training LMS Event Producer

Continuously generates LMS events and publishes them to Kafka.
Implements reliable producer settings: acks=all, retries, batching, compression.
Events are keyed by user_id to achieve deterministic partitioning.
Schema is stable and versioned for downstream compatibility.
Graceful shutdown ensures flush before exit. Logs are structured JSON.
"""

import os
import sys
import json
import time
import uuid
import signal
import random
from datetime import datetime, timezone

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "lms_events")

EVENTS_PER_SECOND = float(os.getenv("EVENTS_PER_SECOND", "5"))
MAX_EVENTS = int(os.getenv("MAX_EVENTS", "0"))  # 0 = infinite

COMPRESSION = os.getenv("KAFKA_PRODUCER_COMPRESSION", "gzip")
BATCH_SIZE = int(os.getenv("KAFKA_BATCH_SIZE_BYTES", str(128 * 1024)))
LINGER_MS = int(os.getenv("KAFKA_LINGER_MS", "20"))
FLUSH_EVERY = int(os.getenv("FLUSH_EVERY", "100"))
KEY_BY = os.getenv("KEY_BY", "user_id")
SCHEMA_VERSION = os.getenv("SCHEMA_VERSION", "v1")

# ------------------------------------------------------------
# Logging and shutdown
# ------------------------------------------------------------
_running = True

def _log(level, **fields):
    msg = {"ts": datetime.now(timezone.utc).isoformat(), "level": level, **fields}
    print(json.dumps(msg), flush=True)

def _handle_sigterm(signum, frame):
    global _running
    _log("INFO", msg="signal_received", signum=signum)
    _running = False

signal.signal(signal.SIGINT, _handle_sigterm)
signal.signal(signal.SIGTERM, _handle_sigterm)

# ------------------------------------------------------------
# Event model
# ------------------------------------------------------------
fake = Faker()

COURSES = [
    "ct_anatomy_fundamentals",
    "ct_physics_technology",
    "radiation_safety_practices",
    "image_acquisition_procedures",
    "contrast_administration_protocols",
    "patient_positioning_techniques",
    "quality_assurance_procedures",
    "ct_pathology_recognition",
]

EVENT_TYPES = [
    "course_enrolled",
    "video_started",
    "video_completed",
    "quiz_attempted",
    "quiz_submitted",
    "article_viewed",
]

def rand_user_id():
    return f"ct_student_{random.randint(1000, 1999)}"

def rand_course_id():
    return random.choice(COURSES)

def now_utc_iso():
    return datetime.now(timezone.utc).isoformat()

def now_epoch_ms():
    return int(time.time() * 1000)

def build_payload(event_type: str):
    if event_type == "course_enrolled":
        return {
            "enroll_method": random.choice(["self_service", "assigned"]),
            "cohort": random.choice(["A", "B", "C"]),
        }
    elif event_type == "video_started":
        return {
            "video_id": f"v{random.randint(1000,9999)}",
            "position_seconds": 0,
        }
    elif event_type == "video_completed":
        return {
            "video_id": f"v{random.randint(1000,9999)}",
            "watch_seconds": random.randint(30, 1800),
        }
    elif event_type == "quiz_attempted":
        return {
            "quiz_id": f"q{random.randint(100,999)}",
            "questions": random.randint(5, 20),
        }
    elif event_type == "quiz_submitted":
        return {
            "quiz_id": f"q{random.randint(100,999)}",
            "score": round(random.uniform(0, 100), 1),
            "passed": random.choice([True, False]),
        }
    elif event_type == "article_viewed":
        return {
            "article_id": f"a{random.randint(1000,9999)}",
            "read_seconds": random.randint(10, 900),
        }
    else:
        return {}

# ------------------------------------------------------------
# Kafka Producer
# ------------------------------------------------------------
def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        retries=10,
        linger_ms=LINGER_MS,
        batch_size=BATCH_SIZE,
        compression_type=COMPRESSION,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

# ------------------------------------------------------------
# Main loop
# ------------------------------------------------------------
def main():
    producer = build_producer()
    _log("INFO", msg="producer_started",
         bootstrap=BOOTSTRAP, topic=TOPIC,
         eps=EVENTS_PER_SECOND, compression=COMPRESSION)

    sent = 0
    sleep_seconds = 1.0 / max(EVENTS_PER_SECOND, 0.001)

    try:
        while _running and (MAX_EVENTS == 0 or sent < MAX_EVENTS):
            event_type = random.choice(EVENT_TYPES)
            user_id = rand_user_id()
            course_id = rand_course_id()

            event = {
                "schema_version": SCHEMA_VERSION,
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "user_id": user_id,
                "course_id": course_id,
                "timestamp_iso": now_utc_iso(),
                "timestamp_ms": now_epoch_ms(),
                "payload": build_payload(event_type),
            }

            key = event[KEY_BY] if KEY_BY in event else user_id

            future = producer.send(TOPIC, key=key, value=event)

            def on_success(metadata):
                _log("DEBUG", msg="delivered",
                     topic=metadata.topic, partition=metadata.partition,
                     offset=metadata.offset, event_id=event["event_id"])

            def on_error(ex: KafkaError):
                _log("ERROR", msg="delivery_failed",
                     error=str(ex), event_id=event["event_id"])

            future.add_callback(on_success)
            future.add_errback(on_error)

            sent += 1
            if sent % FLUSH_EVERY == 0:
                producer.flush(timeout=10)

            time.sleep(sleep_seconds)

    except KeyboardInterrupt:
        _log("INFO", msg="keyboard_interrupt")
    finally:
        producer.flush(timeout=10)
        producer.close(timeout=10)
        _log("INFO", msg="producer_stopped", sent=sent)

if __name__ == "__main__":
    if not TOPIC:
        _log("ERROR", msg="missing_topic_env")
        sys.exit(2)
    main()