# 22. API Specifications

This specification defines the public REST contract exposed through `api-gateway` under `/api/v1`. All application endpoints require `Authorization: Bearer <jwt>` unless explicitly marked as Twilio webhook or public authentication. The live voice hot path remains internal gRPC bidirectional streaming or WebSocket only between `media-gateway-service`, `stt-adapter-service`, `ai-orchestrator-service`, and `tts-adapter-service`; REST is for control-plane, warm-path, and business APIs.

## 22.1 Cross-cutting API Contract

### Transport, authentication, tenancy

| Concern | Standard |
|---|---|
| Base URL | `https://{tenant-domain}/api/v1` through `api-gateway` |
| Authentication | JWT bearer tokens issued by `identity-service`; access tokens expire in 15 minutes |
| Authorization | RBAC and ABAC using `orgId`, `role`, `scope`, and data-scope claims |
| Tenant isolation | `orgId` from token; request bodies must not override tenant context |
| ID format | UUIDv7 for `Call`, `Conversation`, `Ticket`, `Appointment`, `Campaign`, `KnowledgeDocument`, and related entities |
| Content type | `application/json`; uploads use `multipart/form-data` with JSON metadata part |
| Time format | ISO-8601 UTC timestamps |
| Traceability | Every response includes `X-Trace-Id`; client may provide `X-Correlation-Id` |

### Standard headers

| Header | Direction | Required | Description |
|---|---:|---:|---|
| `Authorization: Bearer <jwt>` | Request | Yes except webhooks and token issue | OAuth2 access token |
| `Idempotency-Key` | Request | Required for POST commands that create external side effects | Client-generated UUID or stable action key; retained for 24 hours minimum |
| `X-Correlation-Id` | Request | Optional | Propagated to logs, spans, Kafka metadata, and downstream calls |
| `X-Trace-Id` | Response | Always | OpenTelemetry trace id or gateway-generated id |
| `RateLimit-Limit` | Response | Always | Tenant plus route limit for the current window |
| `RateLimit-Remaining` | Response | Always | Remaining requests in the current window |
| `RateLimit-Reset` | Response | Always | Epoch seconds when the current window resets |
| `Retry-After` | Response | On 429 or 503 | Seconds or HTTP date |
| `ETag` | Response | On mutable resources | Version for optimistic updates |
| `If-Match` | Request | On PATCH where practical | Prevents lost updates |

### Pagination

List APIs use cursor pagination. Offset pagination is prohibited for high-cardinality data such as `Call`, `Conversation`, `Ticket`, and `CampaignContact`.

```http
GET /api/v1/tickets?status=open&limit=50&cursor=eyJpZCI6...
```

```json
{
  "data": [],
  "page": {
    "limit": 50,
    "nextCursor": "eyJpZCI6...",
    "hasMore": true
  }
}
```

### Standard error envelope

All errors use RFC 7807 `application/problem+json` with a mandatory `traceId` and stable `code`.

```json
{
  "type": "https://docs.voxagent.example/problems/VALIDATION_ERROR",
  "title": "Validation failed",
  "status": 400,
  "detail": "phoneNumber must be E.164 formatted",
  "instance": "/api/v1/calls/outbound",
  "code": "VALIDATION_ERROR",
  "traceId": "7f2d6c0b7d2a4d6d8b1b55d7ed54a0a9",
  "errors": [
    { "field": "phoneNumber", "reason": "must match E.164" }
  ]
}
```

### Error code catalog

| Code | HTTP | Retry | Typical source |
|---|---:|---:|---|
| `VALIDATION_ERROR` | 400 | No | Schema, enum, format, business rule failure |
| `UNAUTHENTICATED` | 401 | After re-auth | Missing, expired, or invalid JWT |
| `FORBIDDEN` | 403 | No | RBAC or ABAC denied |
| `NOT_FOUND` | 404 | No | Resource absent or outside tenant scope |
| `CONFLICT` | 409 | No | State transition conflict or duplicate resource |
| `IDEMPOTENCY_CONFLICT` | 409 | No | Same `Idempotency-Key` reused with different body |
| `PRECONDITION_FAILED` | 412 | Refetch | `If-Match` did not match current `ETag` |
| `PAYLOAD_TOO_LARGE` | 413 | With smaller payload | Document, bulk contact, or transcript payload too large |
| `UNSUPPORTED_MEDIA_TYPE` | 415 | No | Unsupported upload type |
| `RATE_LIMITED` | 429 | Yes | Per-tenant, per-route, or per-vendor budget exhausted |
| `TWILIO_SIGNATURE_INVALID` | 401 | No | Webhook signature validation failed |
| `VENDOR_TIMEOUT` | 504 | Yes with backoff | Twilio, Deepgram, Azure OpenAI, ElevenLabs, Azure Speech TTS |
| `DEPENDENCY_UNAVAILABLE` | 503 | Yes | PostgreSQL, Redis, Kafka, or downstream service unavailable |
| `INTERNAL_ERROR` | 500 | Yes | Unhandled server failure |

### Architecture Review (self-critique)

The API contract is intentionally conservative and enterprise-friendly, but it increases implementation burden before product-market fit. Cursor pagination, strict idempotency, RFC 7807, rate-limit headers, and ETags are correct for scale, yet MVP teams often skip them and accumulate compatibility debt. The risk is not the standard; the risk is uneven enforcement across services. Contract tests at `api-gateway` and generated SDKs should be mandatory.

## 22.2 `call-management-service` APIs

`call-management-service` owns `Call` lifecycle commands and Twilio webhook ingestion. It does not process media frames; it coordinates Twilio and `media-gateway-service` setup.

### Endpoint: `POST /calls/outbound`

Creates an outbound `Call` request. Requires `Idempotency-Key`.

Request:
```json
{
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "toPhoneNumber": "+14155550123",
  "fromPhoneNumber": "+14155550999",
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "campaignId": "018f8a73-a118-72fa-9f20-daf52e333333",
  "metadata": { "leadId": "018f8a73-bbbb-7000-9000-111111111111" }
}
```

Response `202 Accepted`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "status": "queued",
  "direction": "outbound",
  "twilioCallSid": null,
  "createdAt": "2026-09-24T17:28:11Z"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `RATE_LIMITED`, `DEPENDENCY_UNAVAILABLE`, or `IDEMPOTENCY_CONFLICT`.

### Endpoint: `GET /calls/{id}`

Fetches a tenant-scoped `Call`.

Request: path parameter `id`.

Response `200 OK`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "direction": "outbound",
  "status": "in_progress",
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "startedAt": "2026-09-24T17:28:42Z",
  "endedAt": null,
  "durationSeconds": null
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND`, `FORBIDDEN`, or `UNAUTHENTICATED`.

### Endpoint: `POST /calls/{id}/transfer`

Transfers an active `Call` to a human destination. Requires `Idempotency-Key`.

Request:
```json
{
  "targetType": "queue",
  "queueId": "sales-tier-1",
  "phoneNumber": null,
  "contextPackage": {
    "summary": "Caller wants pricing for enterprise plan",
    "sentiment": "neutral",
    "priority": "normal"
  }
}
```

Response `202 Accepted`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "transferId": "018f8a75-9912-74db-b832-001122334455",
  "status": "transfer_requested",
  "conferenceName": "voxagent-018f8a74"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT` if the call is not active, `NOT_FOUND`, or `VENDOR_TIMEOUT`.

### Endpoint: `POST /calls/{id}/hangup`

Ends an active `Call`. Requires `Idempotency-Key`.

Request:
```json
{
  "reason": "agent_completed",
  "note": "Customer received appointment confirmation"
}
```

Response `202 Accepted`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "status": "hangup_requested"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT`, `NOT_FOUND`, or `VENDOR_TIMEOUT`.

### Endpoint: `POST /webhooks/twilio/voice`

Twilio voice webhook. This endpoint bypasses JWT but requires Twilio signature validation using `X-Twilio-Signature`; the public URL is isolated and rate limited.

Request:
```json
{
  "CallSid": "CA123",
  "AccountSid": "AC123",
  "From": "+14155550123",
  "To": "+14155550999",
  "CallStatus": "ringing",
  "Direction": "inbound"
}
```

Response `200 OK` with TwiML:
```xml
<Response>
  <Connect>
    <Stream url="wss://voice.example.com/media/twilio/CA123" />
  </Connect>
</Response>
```

Error payload: standard RFC 7807 envelope with `TWILIO_SIGNATURE_INVALID`, `RATE_LIMITED`, or `DEPENDENCY_UNAVAILABLE`.

### Endpoint: `POST /webhooks/twilio/status`

Twilio call status callback. Signature validation is mandatory.

Request:
```json
{
  "CallSid": "CA123",
  "CallStatus": "completed",
  "CallDuration": "284",
  "RecordingUrl": "https://api.twilio.com/recordings/RE123"
}
```

Response `204 No Content`.

Error payload: standard RFC 7807 envelope with `TWILIO_SIGNATURE_INVALID`, `VALIDATION_ERROR`, or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

The API hides Twilio complexity well, but the webhook shape is operationally fragile: Twilio commonly posts form-encoded payloads, not JSON. Production must support `application/x-www-form-urlencoded` at the edge while documenting normalized JSON internally. The transfer API also assumes queue semantics before the human console exists; this should be feature-flagged during MVP.

## 22.3 `conversation-service` APIs

`conversation-service` reads asynchronously materialized `Conversation`, `ConversationTurn`, `Transcript`, and `CallSummary` data populated from `conversation.turns.v1`, `call.media.metadata.v1`, and `conversation.completed.v1`.

### Endpoint: `GET /conversations/{callId}`

Request: path parameter `callId`.

Response `200 OK`:
```json
{
  "conversationId": "018f8a76-6f5e-700e-9c62-2b2222222222",
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "status": "completed",
  "language": "en-US",
  "startedAt": "2026-09-24T17:28:42Z",
  "completedAt": "2026-09-24T17:33:28Z",
  "turnCount": 8,
  "sentiment": "positive"
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND` or `FORBIDDEN`.

### Endpoint: `GET /conversations/{callId}/transcript`

Request: path parameter `callId`; optional query `redaction=applied|raw` where `raw` requires elevated scope.

Response `200 OK`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "redaction": "applied",
  "turns": [
    {
      "turnId": "018f8a77-1111-7000-8000-aaaaaaaaaaaa",
      "speaker": "customer",
      "text": "I need to book an appointment",
      "startedAtMs": 1200,
      "endedAtMs": 3100,
      "confidence": 0.94
    }
  ]
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND`, `FORBIDDEN`, or `PRECONDITION_FAILED` if transcript redaction is still processing.

### Endpoint: `GET /conversations/{callId}/summary`

Request: path parameter `callId`.

Response `200 OK`:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "summary": "Customer booked a dental cleaning for Friday at 10 AM.",
  "disposition": "appointment_booked",
  "followUpActions": [
    { "type": "send_sms", "status": "completed" }
  ],
  "generatedAt": "2026-09-24T17:34:02Z"
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND` or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

The read model is intentionally asynchronous, so callers may see lag immediately after hangup. That is acceptable for dashboards but not for agent handoff; human escalation must stream context directly from `ai-orchestrator-service` or `media-gateway-service` state, not wait for `conversation-service` projections.

## 22.4 `ticket-service` APIs

`ticket-service` owns `Ticket` records and emits `ticket.events.v1`. Creation is idempotent and may originate from REST or `ai.actions.v1`.

### Endpoint: `POST /tickets`

Request:
```json
{
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "title": "Billing question",
  "description": "Customer reports duplicate charge",
  "priority": "high",
  "externalActionId": "act_018f8a78"
}
```

Response `201 Created`:
```json
{
  "ticketId": "018f8a79-5b8a-7777-9d9d-555555555555",
  "ticketNumber": "TCK-2026-000123",
  "status": "open",
  "createdAt": "2026-09-24T17:31:11Z"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `IDEMPOTENCY_CONFLICT`, or `CONFLICT`.

### Endpoint: `GET /tickets`

Request: optional query `status`, `priority`, `customerId`, `limit`, `cursor`.

Response `200 OK`:
```json
{
  "data": [
    {
      "ticketId": "018f8a79-5b8a-7777-9d9d-555555555555",
      "ticketNumber": "TCK-2026-000123",
      "title": "Billing question",
      "status": "open",
      "priority": "high"
    }
  ],
  "page": { "limit": 50, "nextCursor": null, "hasMore": false }
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `FORBIDDEN`, or `RATE_LIMITED`.

### Endpoint: `GET /tickets/{id}`

Request: path parameter `id`.

Response `200 OK`:
```json
{
  "ticketId": "018f8a79-5b8a-7777-9d9d-555555555555",
  "ticketNumber": "TCK-2026-000123",
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "status": "open",
  "priority": "high",
  "description": "Customer reports duplicate charge",
  "createdAt": "2026-09-24T17:31:11Z",
  "updatedAt": "2026-09-24T17:31:11Z"
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND` or `FORBIDDEN`.

### Endpoint: `PATCH /tickets/{id}`

Request:
```json
{
  "status": "resolved",
  "priority": "normal",
  "assigneeUserId": "018f8a7a-5555-7000-8000-bbbbbbbbbbbb",
  "resolution": "Refund initiated"
}
```

Response `200 OK`:
```json
{
  "ticketId": "018f8a79-5b8a-7777-9d9d-555555555555",
  "ticketNumber": "TCK-2026-000123",
  "status": "resolved",
  "updatedAt": "2026-09-24T17:45:00Z"
}
```

Error payload: standard RFC 7807 envelope with `PRECONDITION_FAILED`, `CONFLICT`, or `NOT_FOUND`.

### Architecture Review (self-critique)

The API is simple enough, but ownership boundaries with external CRMs are under-specified. If Salesforce or HubSpot becomes the source of truth for tickets, `ticket-service` becomes a cache plus command facade. The current design assumes VoxAgent owns the workflow; enterprise integrations may force a reversal.

## 22.5 `scheduling-service` APIs

`scheduling-service` owns `Appointment` lifecycle and availability calculations. Redis slot holds are internal implementation details but exposed through deterministic booking behavior.

### Endpoint: `GET /availability`

Request query:
```json
{
  "resourceId": "dentist-01",
  "serviceType": "cleaning",
  "from": "2026-09-25T00:00:00Z",
  "to": "2026-09-30T00:00:00Z",
  "timezone": "America/Los_Angeles"
}
```

Response `200 OK`:
```json
{
  "resourceId": "dentist-01",
  "slots": [
    {
      "slotId": "slot_20260925_1700_dentist_01",
      "startsAt": "2026-09-25T17:00:00Z",
      "endsAt": "2026-09-25T17:30:00Z",
      "holdExpiresAt": null
    }
  ]
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR` or `DEPENDENCY_UNAVAILABLE`.

### Endpoint: `POST /appointments`

Request:
```json
{
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "slotId": "slot_20260925_1700_dentist_01",
  "serviceType": "cleaning",
  "notes": "First visit"
}
```

Response `201 Created`:
```json
{
  "appointmentId": "018f8a7b-4c5d-7000-9000-666666666666",
  "status": "booked",
  "startsAt": "2026-09-25T17:00:00Z",
  "endsAt": "2026-09-25T17:30:00Z",
  "confirmationCode": "APT-582194"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT` if the slot is unavailable, `IDEMPOTENCY_CONFLICT`, or `VALIDATION_ERROR`.

### Endpoint: `PATCH /appointments/{id}/cancel`

Request:
```json
{
  "reason": "customer_requested",
  "notifyCustomer": true
}
```

Response `200 OK`:
```json
{
  "appointmentId": "018f8a7b-4c5d-7000-9000-666666666666",
  "status": "cancelled",
  "cancelledAt": "2026-09-24T18:02:00Z"
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND`, `CONFLICT`, or `PRECONDITION_FAILED`.

### Architecture Review (self-critique)

Availability looks like a read API but is actually a race-prone reservation workflow. The design must separate display-only slots from held slots and must expire Redis holds safely. Redis is not a source of truth under D3, so final booking must be protected by PostgreSQL unique constraints on resource and time range.

## 22.6 `campaign-service` APIs

`campaign-service` owns `Campaign`, `Lead`, and `CampaignContact` orchestration and publishes `campaign.dial.commands.v1`.

### Endpoint: `POST /campaigns`

Request:
```json
{
  "name": "September renewals",
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "fromPhoneNumber": "+14155550999",
  "schedule": { "timezone": "America/New_York", "windows": ["09:00-17:00"] },
  "pacing": { "maxConcurrentCalls": 50, "abandonRateTargetPct": 2.0 }
}
```

Response `201 Created`:
```json
{
  "campaignId": "018f8a7c-7777-7000-8000-777777777777",
  "status": "draft",
  "createdAt": "2026-09-24T18:05:00Z"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR` or `IDEMPOTENCY_CONFLICT`.

### Endpoint: `POST /campaigns/{id}/contacts:bulk`

Request:
```json
{
  "contacts": [
    {
      "externalId": "lead-1001",
      "name": "Ada Lovelace",
      "phoneNumber": "+14155550123",
      "metadata": { "segment": "renewal" }
    }
  ]
}
```

Response `202 Accepted`:
```json
{
  "campaignId": "018f8a7c-7777-7000-8000-777777777777",
  "accepted": 1,
  "rejected": 0,
  "importJobId": "018f8a7d-8888-7000-8000-888888888888"
}
```

Error payload: standard RFC 7807 envelope with `PAYLOAD_TOO_LARGE`, `VALIDATION_ERROR`, or `NOT_FOUND`.

### Endpoint: `POST /campaigns/{id}/start`

Request:
```json
{
  "startAt": "2026-09-25T13:00:00Z",
  "respectQuietHours": true
}
```

Response `202 Accepted`:
```json
{
  "campaignId": "018f8a7c-7777-7000-8000-777777777777",
  "status": "starting"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT`, `NOT_FOUND`, or `RATE_LIMITED`.

### Endpoint: `POST /campaigns/{id}/pause`

Request:
```json
{
  "reason": "operator_requested"
}
```

Response `202 Accepted`:
```json
{
  "campaignId": "018f8a7c-7777-7000-8000-777777777777",
  "status": "pausing"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT` or `NOT_FOUND`.

### Endpoint: `GET /campaigns/{id}/stats`

Request: path parameter `id`.

Response `200 OK`:
```json
{
  "campaignId": "018f8a7c-7777-7000-8000-777777777777",
  "status": "running",
  "contactsTotal": 10000,
  "callsAttempted": 2450,
  "callsConnected": 1180,
  "conversionRatePct": 8.4,
  "abandonRatePct": 1.2
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND` or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

Campaign APIs imply predictive dialing governance, consent, quiet hours, and abandonment-rate controls. Those obligations are not optional; without them, this service becomes a compliance liability. MVP should avoid outbound campaigns until consent and audit flows are proven.

## 22.7 `knowledge-service` APIs

`knowledge-service` owns `KnowledgeSource`, `KnowledgeDocument`, `DocumentChunk`, embeddings, and hybrid search over PostgreSQL 16 plus pgvector. It emits `knowledge.ingestion.v1` for ingestion workers.

### Endpoint: `POST /knowledge/sources`

Request:
```json
{
  "name": "Support Center",
  "type": "url",
  "uri": "https://example.com/help",
  "syncPolicy": { "mode": "scheduled", "cron": "0 */6 * * *" }
}
```

Response `201 Created`:
```json
{
  "knowledgeSourceId": "018f8a7e-9999-7000-8000-999999999999",
  "status": "pending_ingestion"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `FORBIDDEN`, or `IDEMPOTENCY_CONFLICT`.

### Endpoint: `POST /knowledge/documents`

Upload endpoint using `multipart/form-data` with `file` and `metadata` parts.

Request metadata part:
```json
{
  "knowledgeSourceId": "018f8a7e-9999-7000-8000-999999999999",
  "title": "Refund policy",
  "contentType": "application/pdf",
  "tags": ["billing", "refunds"]
}
```

Response `202 Accepted`:
```json
{
  "knowledgeDocumentId": "018f8a7f-aaaa-7000-8000-aaaaaaaaaaaa",
  "status": "queued",
  "ingestionTopic": "knowledge.ingestion.v1"
}
```

Error payload: standard RFC 7807 envelope with `PAYLOAD_TOO_LARGE`, `UNSUPPORTED_MEDIA_TYPE`, or `DEPENDENCY_UNAVAILABLE`.

### Endpoint: `GET /knowledge/search`

Request query represented as JSON for documentation:
```json
{
  "q": "What is the refund window?",
  "topK": 5,
  "filters": { "tags": ["billing"] },
  "mode": "hybrid"
}
```

Response `200 OK`:
```json
{
  "query": "What is the refund window?",
  "results": [
    {
      "documentId": "018f8a7f-aaaa-7000-8000-aaaaaaaaaaaa",
      "chunkId": "018f8a80-bbbb-7000-8000-bbbbbbbbbbbb",
      "score": 0.91,
      "text": "Refunds are available within 30 days...",
      "citation": { "title": "Refund policy", "uri": "s3://tenant/docs/refunds.pdf", "page": 2 }
    }
  ]
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `RATE_LIMITED`, or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

Exposing search directly is useful for testing and admin tooling, but the conversational hot path should call `knowledge-service` over internal gRPC to avoid REST overhead and to support cancellation, deadlines, and streaming rerank results. pgvector is the right starting point under D4, not a permanent bet.

## 22.8 `notification-service` APIs

`notification-service` sends SMS, email, and future push notifications. Most production traffic should arrive through `notification.commands.v1`; REST is for control-plane and admin-triggered messages.

### Endpoint: `POST /notifications`

Request:
```json
{
  "recipientId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "channel": "sms",
  "templateId": "appointment-confirmation",
  "locale": "en-US",
  "parameters": {
    "appointmentTime": "Friday 10 AM",
    "confirmationCode": "APT-582194"
  }
}
```

Response `202 Accepted`:
```json
{
  "notificationId": "018f8a81-cccc-7000-8000-cccccccccccc",
  "status": "queued"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `RATE_LIMITED`, or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

The endpoint is intentionally generic, but generic notification APIs are easy to abuse. Templates must be versioned, approved, and tenant-scoped. Free-form body sends should be restricted to privileged roles or disallowed entirely for regulated tenants.

## 22.9 `identity-service` APIs

`identity-service` owns `Organization`, `User`, auth tokens, and `IdentityVerification` flows.

### Endpoint: `POST /auth/token`

Public token endpoint using OAuth2-compatible grant shapes.

Request:
```json
{
  "grantType": "password",
  "username": "admin@example.com",
  "password": "correct-horse-battery-staple",
  "orgSlug": "acme"
}
```

Response `200 OK`:
```json
{
  "accessToken": "eyJhbGciOiJSUzI1NiIs...",
  "tokenType": "Bearer",
  "expiresIn": 900,
  "refreshToken": "def50200...",
  "scope": "calls:write tickets:read"
}
```

Error payload: standard RFC 7807 envelope with `UNAUTHENTICATED`, `FORBIDDEN`, or `RATE_LIMITED`.

### Endpoint: `POST /users`

Request:
```json
{
  "email": "agent@example.com",
  "displayName": "Human Agent",
  "roles": ["agent"],
  "status": "invited"
}
```

Response `201 Created`:
```json
{
  "userId": "018f8a82-dddd-7000-8000-dddddddddddd",
  "email": "agent@example.com",
  "status": "invited"
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT`, `VALIDATION_ERROR`, or `FORBIDDEN`.

### Endpoint: `POST /identity-verifications`

Starts a customer verification flow.

Request:
```json
{
  "customerId": "018f8a71-3f1b-7c40-8f12-58b1c2fd1111",
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "method": "sms_otp",
  "phoneNumber": "+14155550123"
}
```

Response `201 Created`:
```json
{
  "identityVerificationId": "018f8a83-eeee-7000-8000-eeeeeeeeeeee",
  "status": "challenge_sent",
  "expiresAt": "2026-09-24T18:15:00Z"
}
```

Error payload: standard RFC 7807 envelope with `RATE_LIMITED`, `VALIDATION_ERROR`, or `CONFLICT`.

### Endpoint: `POST /identity-verifications/{id}/verify`

Request:
```json
{
  "code": "123456"
}
```

Response `200 OK`:
```json
{
  "identityVerificationId": "018f8a83-eeee-7000-8000-eeeeeeeeeeee",
  "status": "verified",
  "verifiedAt": "2026-09-24T18:10:42Z"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `CONFLICT`, or `RATE_LIMITED`.

### Architecture Review (self-critique)

A native password grant is a poor default for enterprise. Production should prefer OIDC authorization code with PKCE through Entra ID, Keycloak, or customer IdPs. The documented shape is useful for internal automation and early tenants but should not become the strategic login model.

## 22.10 `config-service` APIs

`config-service` owns `AIAgentDefinition`, prompt versions, flow configuration, tenant feature flags, and safe rollout controls.

### Endpoint: `POST /agent-definitions`

Request:
```json
{
  "name": "Front Desk Agent",
  "language": "en-US",
  "voice": { "provider": "azure", "voiceId": "en-US-JennyNeural" },
  "tools": ["createTicket", "bookAppointment", "handoffToHuman"],
  "guardrails": { "piiRedaction": true, "maxToolCallsPerTurn": 3 }
}
```

Response `201 Created`:
```json
{
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "version": 1,
  "status": "draft"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `CONFLICT`, or `FORBIDDEN`.

### Endpoint: `GET /agent-definitions`

Request: optional query `status`, `language`, `limit`, `cursor`.

Response `200 OK`:
```json
{
  "data": [
    {
      "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
      "name": "Front Desk Agent",
      "version": 1,
      "status": "draft"
    }
  ],
  "page": { "limit": 50, "nextCursor": null, "hasMore": false }
}
```

Error payload: standard RFC 7807 envelope with `FORBIDDEN` or `RATE_LIMITED`.

### Endpoint: `GET /agent-definitions/{id}`

Request: path parameter `id`.

Response `200 OK`:
```json
{
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "name": "Front Desk Agent",
  "version": 1,
  "status": "draft",
  "tools": ["createTicket", "bookAppointment", "handoffToHuman"]
}
```

Error payload: standard RFC 7807 envelope with `NOT_FOUND` or `FORBIDDEN`.

### Endpoint: `PATCH /agent-definitions/{id}`

Request:
```json
{
  "name": "Front Desk Agent v2",
  "voice": { "provider": "elevenlabs", "voiceId": "premium-voice-01" },
  "guardrails": { "piiRedaction": true, "maxToolCallsPerTurn": 2 }
}
```

Response `200 OK`:
```json
{
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "version": 2,
  "status": "draft"
}
```

Error payload: standard RFC 7807 envelope with `PRECONDITION_FAILED`, `NOT_FOUND`, or `VALIDATION_ERROR`.

### Endpoint: `DELETE /agent-definitions/{id}`

Request: path parameter `id`.

Response `204 No Content`.

Error payload: standard RFC 7807 envelope with `CONFLICT` if active calls or campaigns reference it, `NOT_FOUND`, or `FORBIDDEN`.

### Endpoint: `POST /agent-definitions/{id}/prompt-versions`

Request:
```json
{
  "name": "refund-policy-grounding",
  "systemPrompt": "You are a helpful front desk agent...",
  "toolPolicy": { "allowedTools": ["createTicket", "searchKnowledge"] },
  "evaluationSetId": "eval-frontdesk-v1"
}
```

Response `201 Created`:
```json
{
  "promptVersionId": "018f8a84-ffff-7000-8000-ffffffffffff",
  "agentDefinitionId": "018f8a72-9a77-7be0-a7e4-ff824c317222",
  "version": 3,
  "status": "draft"
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR`, `CONFLICT`, or `FORBIDDEN`.

### Architecture Review (self-critique)

`config-service` is strategically important but easily becomes a dumping ground. Agent definitions, prompts, feature flags, and flow graphs require separate internal modules and approval workflows. The missing piece is a prompt evaluation and regression harness; without it, prompt versioning is merely storage, not governance.

## 22.11 `agent-routing-service` APIs

`agent-routing-service` owns `HumanAgent` availability, queue lookup, and handoff coordination. It emits `escalation.events.v1`.

### Endpoint: `GET /queues`

Request query:
```json
{
  "skill": "billing",
  "language": "en-US",
  "priority": "high"
}
```

Response `200 OK`:
```json
{
  "queues": [
    {
      "queueId": "billing-tier-1",
      "displayName": "Billing Tier 1",
      "availableAgents": 4,
      "estimatedWaitSeconds": 45
    }
  ]
}
```

Error payload: standard RFC 7807 envelope with `VALIDATION_ERROR` or `DEPENDENCY_UNAVAILABLE`.

### Endpoint: `POST /handoffs`

Request:
```json
{
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "queueId": "billing-tier-1",
  "reason": "caller_requested_human",
  "contextPackage": {
    "summary": "Customer disputes duplicate charge",
    "transcriptSoFar": "Agent: How can I help...",
    "sentiment": "frustrated",
    "recommendedAction": "Review last invoice"
  }
}
```

Response `202 Accepted`:
```json
{
  "handoffId": "018f8a85-abcd-7000-8000-123456789abc",
  "callId": "018f8a74-2f1c-7d10-b412-84b9d1c44444",
  "status": "queued",
  "estimatedWaitSeconds": 45
}
```

Error payload: standard RFC 7807 envelope with `CONFLICT`, `NOT_FOUND`, `RATE_LIMITED`, or `DEPENDENCY_UNAVAILABLE`.

### Architecture Review (self-critique)

The service API is only half the product. A human agent console is a missing first-class component: queue state, screen-pop, transcript streaming, whisper notes, and disposition capture cannot be solved by REST endpoints alone. Until the console exists, handoff is operationally incomplete.

## 22.12 Versioning and Internal Protocol Boundary

### URI versioning versus header versioning

The public contract uses URI versioning with `/api/v1` because enterprise customers, API gateways, WAFs, documentation portals, and generated SDKs handle it predictably. Header versioning is cleaner in theory but harder to observe, cache, debug, and explain. Breaking changes require `/api/v2`; additive fields are allowed in `/api/v1`.

Kafka follows topic major versions such as `call.events.v1`. Internal gRPC APIs should use protobuf package versions and strict backward-compatible evolution.

### Why internal gRPC hot path is not exposed as public API

The hot path has p50 and p95 latency budgets of 800 ms and 1500 ms from end of user speech to first audio. Exposing gRPC streaming directly to external clients would couple customers to unstable media, endpointing, and vendor failover semantics; it would also expand the attack surface for RTP-like workloads. Public REST remains stable and auditable, while internal gRPC and WebSocket paths can be optimized aggressively, drained during deploys, and changed behind `api-gateway` without customer-visible churn.

### Architecture Review (self-critique)

URI versioning is pragmatic, not elegant. The bigger danger is pretending REST can cover every workflow: live audio, partial transcripts, barge-in, and TTS streaming belong behind controlled internal interfaces. Conversely, hiding all streaming internals can make partner integrations harder. A future partner SDK may need a constrained WebSocket API, but it should be a separate product decision, not leakage from the hot path.
