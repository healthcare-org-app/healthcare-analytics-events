# analytics-events-service

analytics-events-service — domain: ai_agents

- **Port:** 9102
- **Language:** Python 3.11 + Flask
- **Database:** `ai_agents` (Postgres, table `analytics_events`)
- **Event bus:** Kafka

## API

| Method    | Path                       |
|-----------|----------------------------|
| GET       | `/api/analytics_events/`          |
| POST      | `/api/analytics_events/`          |
| GET       | `/api/analytics_events/<id>`      |
| PUT/PATCH | `/api/analytics_events/<id>`      |
| DELETE    | `/api/analytics_events/<id>`      |
| GET       | `/health`                  |
| GET       | `/ready`                   |

## Events

**Publishes:** (none)
**Subscribes:** auth.session.started, patient.created, encounter.ended, claim.adjudicated, invoice.paid, device.reading

## HTTP peer dependencies

- `audit-log-service`

## Local dev

```bash
pip install -e ../../libs/py-healthcare-common
pip install -r requirements.txt
cp .env.example .env
(cd ../../infra && docker compose up -d postgres kafka kafka-init)
python -m app.main
```

## Tests

```bash
pytest
```
