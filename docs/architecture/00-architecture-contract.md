# 00 — Architecture Contract (Canonical Vocabulary & Decisions)

> This file is the **single source of truth** for names, decisions, and budgets used across all
> architecture documents. Every other document MUST use these exact names.

## 1. Platform

**Product**: AI Voice Agent Platform ("VoxAgent") — multi-tenant, production-grade platform acting as
Front Desk Executive, Customer Support Agent, Receptionist, and Tele-caller.

**Core stack**: Java 21 (virtual threads + ZGC), Spring Boot 3.3+, Spring AI, Spring Cloud Gateway,
PostgreSQL 16 (+ pgvector), Redis 7, Apache Kafka (KRaft), WebSockets, Docker, Kubernetes, Helm,
Azure OpenAI (primary) / OpenAI (fallback), Twilio (primary telephony) + SIP trunking (scale phase),
Deepgram (primary STT) / Azure Speech (fallback), ElevenLabs (premium TTS) / Azure Speech TTS (default),
OpenTelemetry + Prometheus + Grafana + Loki/ELK + Jaeger/Tempo.

## 2. Canonical Microservices (bounded contexts)

| # | Service | Bounded Context | Latency Class |
|---|---------|-----------------|---------------|
| 1 | `api-gateway` | Edge | control-plane |
| 2 | `call-management-service` | Call Lifecycle | control-plane |
| 3 | `media-gateway-service` | Real-time Media | **hot-path** |
| 4 | `stt-adapter-service` | Speech Recognition | **hot-path** |
| 5 | `tts-adapter-service` | Speech Synthesis | **hot-path** |
| 6 | `ai-orchestrator-service` | Agentic AI / Conversation Brain | **hot-path** |
| 7 | `knowledge-service` | Knowledge & RAG | warm-path |
| 8 | `conversation-service` | Transcripts, Summaries, Sentiment | async |
| 9 | `ticket-service` | Support Tickets | async |
| 10 | `scheduling-service` | Appointments | warm-path |
| 11 | `crm-integration-service` | CRM Sync | async |
| 12 | `notification-service` | SMS/Email | async |
| 13 | `campaign-service` | Outbound Campaigns & Leads | async |
| 14 | `agent-routing-service` | Human Handoff & Escalation | warm-path |
| 15 | `identity-service` | Users, Orgs, AuthN/AuthZ, Identity Verification | control-plane |
| 16 | `config-service` | Tenant/Agent Configuration, Prompts, Flows | control-plane |
| 17 | `analytics-service` | Reporting & Insights | async |

**Hot-path rule (non-negotiable)**: The live voice loop
(`media-gateway ↔ stt-adapter ↔ ai-orchestrator ↔ tts-adapter`) communicates over **gRPC
bidirectional streaming / WebSockets only — NEVER Kafka**. Kafka is used exclusively for async,
post-turn, and post-call events.

## 3. Canonical Kafka Topics

| Topic | Key | Partitions (base) | Retention | Producers → Consumers |
|-------|-----|-------------------|-----------|------------------------|
| `call.events.v1` | callId | 50 | 7d | call-mgmt → conversation, analytics, crm, campaign |
| `call.media.metadata.v1` | callId | 50 | 3d | media-gateway → conversation, analytics |
| `conversation.turns.v1` | callId | 50 | 7d | ai-orchestrator → conversation, analytics |
| `conversation.completed.v1` | callId | 20 | 7d | conversation → notification, crm, analytics |
| `ai.actions.v1` | callId | 20 | 7d | ai-orchestrator → ticket, scheduling, crm, notification |
| `ticket.events.v1` | ticketId | 10 | 7d | ticket → notification, crm, analytics |
| `appointment.events.v1` | appointmentId | 10 | 7d | scheduling → notification, crm, analytics |
| `notification.commands.v1` | recipientId | 10 | 3d | * → notification |
| `crm.sync.commands.v1` | customerId | 10 | 7d | * → crm-integration |
| `campaign.dial.commands.v1` | contactId | 20 | 3d | campaign → call-mgmt |
| `lead.events.v1` | leadId | 10 | 30d | campaign, ai-orchestrator → crm, analytics |
| `escalation.events.v1` | callId | 10 | 7d | ai-orchestrator, agent-routing → analytics, notification |
| `knowledge.ingestion.v1` | documentId | 10 | 3d | knowledge → knowledge (workers) |
| `audit.events.v1` | orgId | 20 | 365d (tiered → S3/Blob) | all → analytics, SIEM |
| `dlq.<consumer-group>` | — | 5 | 14d | per consumer group |

Naming: `<domain>.<event-type>.v<version>`. Schema Registry (Avro/JSON-Schema), backward-compatible
evolution, new major version = new topic.

## 4. Canonical Entities

`Organization`, `User`, `Customer`, `Call`, `Conversation`, `ConversationTurn`, `Transcript`,
`CallSummary`, `Ticket`, `Appointment`, `Lead`, `Campaign`, `CampaignContact`, `HumanAgent`,
`AIAgentDefinition`, `KnowledgeSource`, `KnowledgeDocument`, `DocumentChunk`, `ConsentRecord`,
`FollowUpAction`, `AuditLog`, `IdentityVerification`.

Multi-tenancy: every table carries `org_id` (UUID); enforced via PostgreSQL **Row-Level Security**
plus application-level tenant context. IDs are UUIDv7 (time-ordered).

## 5. Latency Budget (voice-to-voice)

Target: **p50 ≤ 800 ms, p95 ≤ 1500 ms** from end of user speech to first synthesized audio.

| Stage | Budget |
|-------|--------|
| Telephony + network ingress | 50–100 ms |
| VAD / endpointing | 150–250 ms |
| Streaming STT final partial | 100–200 ms |
| AI orchestrator (LLM TTFT, incl. tool-decision) | 250–450 ms |
| TTS time-to-first-byte (streaming) | 100–200 ms |
| Egress + jitter buffer | 50–100 ms |

Techniques mandated: streaming everywhere, sentence-level TTS chunking, speculative/eager LLM
prefill, semantic response cache (Redis), barge-in support, filler acknowledgements for tool calls
> 700 ms.

## 6. Key Decisions (with standing critiques)

| ID | Decision | Rationale | Standing critique / trigger to revisit |
|----|----------|-----------|-----------------------------------------|
| D1 | Kafka over RabbitMQ | Replay, high-throughput event streaming, consumer groups, analytics reuse | Overkill < 1K calls/day; ops cost — mitigated by managed Kafka (MSK/Confluent/Event Hubs) |
| D2 | PostgreSQL over MongoDB | Relational domain, ACID, RLS multi-tenancy, pgvector consolidation | Transcript volume → partitioning + tiered archival to object storage |
| D3 | Redis over Hazelcast | Ubiquity, latency, Spring ecosystem, managed offerings | Not a source of truth; everything in Redis must be reconstructible |
| D4 | pgvector over Pinecone (start) | One less system, txn consistency with metadata, cheap | Revisit at >20M vectors or retrieval p95 >150 ms → dedicated vector DB (Qdrant/Pinecone) |
| D5 | Azure OpenAI primary, OpenAI fallback | Enterprise compliance (SOC2, HIPAA BAA, data residency, no training on data), private networking | Model lag vs OpenAI; abstract via Spring AI `ChatModel` for portability |
| D6 | Twilio primary; SIP/self-hosted media (LiveKit/Jambonz) at scale | Fastest to market, global PSTN | ~$0.014+/min becomes dominant cost at scale; migrate high-volume tenants to SIP trunks Phase 3 |
| D7 | Cascaded STT→LLM→TTS pipeline (not speech-to-speech) | Controllability, guardrails, tool-calling maturity, per-stage vendor swap | Speech-to-speech (e.g., GPT-realtime) cuts latency; adopt behind a `VoicePipeline` abstraction Phase 3+ |
| D8 | Java 21 media gateway (virtual threads + ZGC) | Team stack consistency | JVM in RTP/jitter path is contested; keep media gateway thin (relay + VAD), isolate on dedicated node pool; fallback option: LiveKit/off-the-shelf media server |
| D9 | Multi-tenant shared clusters w/ RLS | Cost efficiency | Enterprise tier gets dedicated schema or dedicated DB; noisy-neighbor guarded by per-tenant rate limits & Kafka quotas |
| D10 | Saga/outbox pattern for cross-service consistency | No distributed transactions | Transactional outbox + Debezium CDC or Spring polling publisher |
| D11 | MCP-ready tool layer | Future-proofing | All agent tools implemented as Spring AI `@Tool` beans behind a `ToolRegistry` that can front MCP servers later |

## 7. Security & Compliance Baseline

OAuth2/OIDC (Keycloak or Entra ID), JWT access tokens (≤15 min) + refresh, RBAC + ABAC (org, role,
data-scope claims), mTLS service mesh (Istio/Linkerd), AES-256 at rest, TLS 1.3 in transit,
Vault/Azure Key Vault for secrets, immutable audit log, PII detection + redaction in transcripts
(Presidio-style pipeline), call-recording consent management, GDPR (RTBF via crypto-shredding /
hard-delete jobs), SOC2 controls, PCI-DSS scope isolation for payment flows (DTMF masking),
Zero-Trust posture.

## 8. Environments & Namespaces

Namespaces: `voice-core` (hot-path), `ai-core`, `business-services`, `data-platform`, `platform`
(observability/gateway). Environments: `dev`, `staging`, `prod`. Hot-path services run on a
dedicated node pool `rt-voice` (taint/toleration, Guaranteed QoS, CPU pinning, no throttling).
