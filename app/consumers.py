"""Kafka consumers for analytics-events-service.

One handler per subscribed topic. Handlers are best-effort logging plus
audit — services override this file to implement real cross-domain behavior.
"""
from __future__ import annotations

import logging

from healthcare_common.audit import emit_audit

log = logging.getLogger("analytics-events-service.consumers")


def register(svc) -> None:
    bus = svc.bus

    @bus.on("auth.session.started")
    def _on_auth_session_started(envelope: dict) -> None:
        log.info("analytics-events-service: received auth.session.started id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.auth.session.started", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("patient.created")
    def _on_patient_created(envelope: dict) -> None:
        log.info("analytics-events-service: received patient.created id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.patient.created", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("encounter.ended")
    def _on_encounter_ended(envelope: dict) -> None:
        log.info("analytics-events-service: received encounter.ended id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.encounter.ended", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("claim.adjudicated")
    def _on_claim_adjudicated(envelope: dict) -> None:
        log.info("analytics-events-service: received claim.adjudicated id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.claim.adjudicated", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("invoice.paid")
    def _on_invoice_paid(envelope: dict) -> None:
        log.info("analytics-events-service: received invoice.paid id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.invoice.paid", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

    @bus.on("device.reading")
    def _on_device_reading(envelope: dict) -> None:
        log.info("analytics-events-service: received device.reading id=%s", envelope.get("id"))
        emit_audit(bus, action="consume.device.reading", actor="system:analytics-events-service",
                   target=None, details={"envelope_id": envelope.get("id")})

