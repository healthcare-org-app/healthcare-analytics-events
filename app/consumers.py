"""Kafka consumers for analytics-events-service.

One handler per subscribed topic. Real handlers write to this service's own
database and/or publish follow-up events; stub handlers just log + audit.
"""
from __future__ import annotations

import logging

from psycopg.types.json import Json

from healthcare_common.audit import emit_audit

log = logging.getLogger("analytics-events-service.consumers")

TABLE = "analytics_events"


def register(svc) -> None:
    bus = svc.bus
    db = svc.db
    clients = svc.clients

    @bus.on("auth.session.started")
    def _on_auth_session_started(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/auth.session.started handler failed: %s", e)
        emit_audit(bus, action="consume.auth.session.started", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/patient.created handler failed: %s", e)
        emit_audit(bus, action="consume.patient.created", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("encounter.ended")
    def _on_encounter_ended(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/encounter.ended handler failed: %s", e)
        emit_audit(bus, action="consume.encounter.ended", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("claim.adjudicated")
    def _on_claim_adjudicated(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/claim.adjudicated handler failed: %s", e)
        emit_audit(bus, action="consume.claim.adjudicated", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/invoice.paid handler failed: %s", e)
        emit_audit(bus, action="consume.invoice.paid", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("device.reading")
    def _on_device_reading(envelope: dict) -> None:
        data = envelope.get("data") or {}
        try:
                    # Append event to this service's timeline table under the associated patient.
                    pid = data.get("patient_id") or data.get("id")
                    db.execute(f"INSERT INTO {TABLE} (data) VALUES (%s)",
                               (Json({"patient_id": pid,
                                      "event_type": envelope.get("event_type"),
                                      "occurred_at": envelope.get("occurred_at"),
                                      "producer": envelope.get("producer"),
                                      "summary": data}),))
        except Exception as e:
            log.exception("analytics-events-service/device.reading handler failed: %s", e)
        emit_audit(bus, action="consume.device.reading", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

