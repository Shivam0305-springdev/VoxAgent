# VoxAgent — AI Voice Agent Platform: Architecture Documentation

Production-grade architecture blueprint for a multi-tenant AI Voice Agent platform
(Front Desk Executive, Customer Support Agent, Receptionist, Tele-caller) built on
Java 21, Spring Boot, Spring AI, PostgreSQL, Redis, Kafka, Kubernetes, Azure OpenAI,
Twilio/SIP, streaming STT/TTS.

> **Read first:** [00-architecture-contract.md](00-architecture-contract.md) — the canonical
> vocabulary (services, Kafka topics, entities), latency budgets, and key decisions D1–D11.
> All other documents conform to it.

## Document Map

| Doc | Sections | Contents |
|-----|----------|----------|
| [00-architecture-contract.md](00-architecture-contract.md) | — | Canonical names, decisions, latency budget, security baseline |
| [01-overview-requirements.md](01-overview-requirements.md) | 1–5 | Executive summary, functional & non-functional requirements, assumptions, risk matrix |
| [02-capacity-tech-evaluation.md](02-capacity-tech-evaluation.md) | 6–7 | Capacity planning (1K/10K/100K concurrent calls), technology evaluations |
| [03-high-level-architecture.md](03-high-level-architecture.md) | 8 | HLD diagrams, hot-path vs cold-path, component responsibilities |
| [04-low-level-design.md](04-low-level-design.md) | 9–10 | Per-service LLD: APIs, resilience, idempotency; microservice breakdown |
| [05-domain-driven-design.md](05-domain-driven-design.md) | 11 | Bounded contexts, aggregates, entities, value objects, domain events |
| [06-data-architecture.md](06-data-architecture.md) | 12–13 | ER diagrams, full database design, partitioning, archival, Redis keys |
| [07-event-driven-architecture.md](07-event-driven-architecture.md) | 14 | Kafka topics, payloads, consumer groups, outbox, event flows |
| [08-ai-agent-architecture.md](08-ai-agent-architecture.md) | 15 | Agent roster, orchestration, Spring AI implementation, state machine, MCP |
| [09-rag-voice-pipeline.md](09-rag-voice-pipeline.md) | 16–17 | RAG ingestion/retrieval/guardrails; real-time voice processing flow |
| [10-security-architecture.md](10-security-architecture.md) | 18 | OAuth2/JWT, RBAC/ABAC, encryption, secrets, PII, GDPR/SOC2, Zero Trust |
| [11-observability-failures.md](11-observability-failures.md) | 19–20 | OTel/Prometheus/Grafana/tracing; failure scenarios & degradation matrix |
| [12-deployment-architecture.md](12-deployment-architecture.md) | 21 | Kubernetes, Helm, CI/CD, blue-green/canary, call draining, multi-region |
| [13-ai-safety-guardrails.md](13-ai-safety-guardrails.md) | 26 | Prompt injection, toxicity, PII, hallucination detection, response validation |
| [14-api-specifications.md](14-api-specifications.md) | 22 | REST API specs, error envelope, versioning |
| [15-sequence-diagrams.md](15-sequence-diagrams.md) | 23 | Incoming/outbound call, ticket, appointment, escalation, knowledge retrieval |
| [16-cost-performance-roadmap.md](16-cost-performance-roadmap.md) | 24–25, 27–29 | Cost analysis, performance optimization, roadmap, architecture review, final recommendation |

## How to Read

- **Executives / product**: 01 → 16 (§24, §27, §29)
- **Engineering leads starting implementation**: 00 → 03 → 04 → 06 → 07 → 14 → 16 (§28–29)
- **AI/ML engineers**: 08 → 09 → 13
- **Security / compliance**: 10 → 13 → 11
- **SRE / platform**: 11 → 12 → 02

## Design Philosophy

1. **Hot path is sacred** — the live voice loop (media ↔ STT ↔ LLM ↔ TTS) is streaming gRPC/WebSocket
   only, never brokered through Kafka; everything else is async and event-driven.
2. **Critique built-in** — every document ends major sections with a self-critique; decisions carry
   explicit "triggers to revisit" (see contract D1–D11).
3. **Start consolidated, split at scale** — §28 recommends a 5-deployable modulith start that
   preserves the 17 bounded contexts as module boundaries.
4. **Vendor abstraction everywhere** — Spring AI `ChatModel`, `VoicePipeline`, STT/TTS adapters, and
   an MCP-ready `ToolRegistry` keep every external dependency swappable.
