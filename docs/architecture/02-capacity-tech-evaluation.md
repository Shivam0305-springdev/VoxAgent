# 6. Capacity Planning

This plan models three concurrency tiers for VoxAgent: 1K, 10K, and 100K concurrent calls. The numbers are planning estimates, not procurement commitments. They assume average call duration of 4 minutes, one conversational turn every 10 seconds per active call, 2,000 input tokens plus 150 output tokens per turn, 8 kHz G.711 mu-law audio at 64 kbps payload per direction, and roughly 25 percent packet and transport overhead.

## 6.1 Capacity assumptions

| Parameter | Value | Rationale |
|---|---:|---|
| Audio payload | 64 kbps per direction | 8 kHz G.711 mu-law over PSTN or SIP media. |
| Audio with overhead | 80 kbps per direction | RTP or WebSocket framing, IP, TLS, jitter, and observability overhead. |
| Bidirectional media | 160 kbps per call | Caller inbound plus synthesized outbound. |
| Engineering network budget | 200 kbps per call | Adds headroom for recording, retransmits, sidecars, and cross-AZ routing. |
| Average call duration | 4 min | Conservative blended front desk and support estimate. |
| Turn rate | 1 turn per 10 s per call | Six turns per minute per active call. |
| LLM token shape | 2,000 input plus 150 output tokens per turn | Includes system prompt, tenant policy, compressed context, RAG excerpts, and tool schema subset. |
| Media pod density | 75 calls per `media-gateway-service` pod | JVM with Netty, virtual threads, ZGC, VAD, and relay only; range is 50 to 100 depending jitter and CPU. |
| AI pod density | 250 active calls per `ai-orchestrator-service` pod | Mostly async IO to LLM, tools, Redis, and RAG, with CPU for policy checks and context compression. |
| Transcript size | 50 KB per call raw | Transcript plus turn metadata before index and summary amplification. |
| Storage amplification | 2.0x | Indexes, summaries, embeddings metadata, audit joins, and partition overhead. |

## 6.2 Voice and network bandwidth

| Concurrent calls | Per-call budget | Aggregate bidirectional bandwidth | With 30 percent regional headroom | Comment |
|---:|---:|---:|---:|---|
| 1K | 200 kbps | 200 Mbps | 260 Mbps | Fits one regional cluster with standard redundant ingress. |
| 10K | 200 kbps | 2 Gbps | 2.6 Gbps | Requires multi-AZ load balancing, pod anti-affinity, and careful east-west traffic control. |
| 100K | 200 kbps | 20 Gbps | 26 Gbps | Requires regional sharding, SIP edge strategy, and dedicated node pools; not a single-cluster target. |

Twilio Media Streams and SIP trunks also impose concurrent stream, CPS, and regional routing limits. The architecture must treat telephony ingress as a quota-managed dependency, not an infinite pipe.

## 6.3 Hot-path service sizing

| Tier | `media-gateway-service` pods | Rationale | `stt-adapter-service` pods | `tts-adapter-service` pods | `ai-orchestrator-service` pods |
|---:|---:|---|---:|---:|---:|
| 1K | 16 | 1,000 divided by 75 plus N+2 headroom. | 12 | 12 | 5 |
| 10K | 150 | 134 base plus zone and rollout headroom. | 100 | 100 | 50 |
| 100K | 1,500 | 1,334 base plus multi-region headroom. | 900 | 900 | 500 |

STT adapters should maintain one inbound streaming STT session per active call. TTS concurrency is burstier because only part of the call is speaking, but provider connection quotas must still be sized for near-call-level bursts during greetings, campaign starts, and incident recovery.

## 6.4 STT and TTS vendor quota implications

| Tier | STT active streams | TTS planned concurrent streams | Vendor concern | Required action |
|---:|---:|---:|---|---|
| 1K | 1K | 300 to 1K | Default enterprise quotas may be insufficient. | Reserve Deepgram and Azure Speech limits before launch tests. |
| 10K | 10K | 3K to 10K | Regional stream quotas and failover capacity become design constraints. | Split across regions and providers; pre-negotiate burst quota. |
| 100K | 100K | 30K to 100K | Standard public API quotas are unlikely to support failover or campaign spikes. | Use committed capacity, multi-provider routing, SIP regional edges, and tenant-level admission control. |

Risk: if STT or TTS fallback capacity is smaller than primary capacity, provider failover becomes partial rather than protective. Alternatives are dedicated vendor capacity, self-hosted speech for commodity languages, or reducing concurrency through queueing and human transfer. Recommendation: require tested fallback capacity for enterprise SLAs and document lower best-effort behavior for lower tiers.

## 6.5 LLM quota math

| Concurrent calls | Turns per second | Input TPM | Output TPM | Total tokens per second | Why this matters |
|---:|---:|---:|---:|---:|---|
| 1K | 100 | 12,000,000 | 900,000 | 215,000 | Already exceeds many default Azure OpenAI deployments. |
| 10K | 1,000 | 120,000,000 | 9,000,000 | 2,150,000 | Requires many deployments, regions, and model tiers. |
| 100K | 10,000 | 1,200,000,000 | 90,000,000 | 21,500,000 | LLM quota is the real bottleneck, not Kubernetes compute. |

At 100K concurrent calls, the hard problem is not pod count. It is LLM throughput, regional quota, token cost, and latency under burst. Required options:

1. Multi-region Azure OpenAI deployment sharding with tenant-aware routing and data residency controls.
2. Multiple model deployments per region with quota-aware load balancing.
3. Azure OpenAI Provisioned Throughput Units or equivalent dedicated capacity for enterprise tenants.
4. OpenAI fallback only where compliance allows, using Spring AI `ChatModel` abstraction per D5.
5. Aggressive prompt compaction, response caching, tool schema pruning, and smaller model routing for routine FAQ turns.
6. Human transfer or async callback when all compliant model pools are saturated.

## 6.6 Kafka throughput

Kafka is not in the live voice loop. It carries async, post-turn, and post-call events through canonical topics such as `call.events.v1`, `conversation.turns.v1`, `ai.actions.v1`, `conversation.completed.v1`, and `audit.events.v1`.

| Tier | Estimated events per second | Estimated ingress | Base partition fit | Planning guidance |
|---:|---:|---:|---|---|
| 1K | 1.5K to 3K | 2 to 5 MB/s | Contract base partitions are adequate. | Three brokers can handle this if disks are fast and schemas are compact. |
| 10K | 15K to 30K | 20 to 50 MB/s | Increase hot topics to 100 to 200 partitions. | Separate analytics consumers from operational consumers; monitor lag by tenant. |
| 100K | 150K to 300K | 200 to 500 MB/s | Increase hot topics to 500 to 1,000 partitions or shard Kafka by region. | Managed Kafka or Event Hubs style service is strongly preferred. |

Risk: Kafka can become a distributed systems tax before the business needs replay. Alternatives are RabbitMQ for command queues or managed event bus. Recommendation: keep Kafka per D1 for replay and analytics, but use managed Kafka and strict schema governance.

## 6.7 PostgreSQL and transcript storage

Calls per month assume 30 days at full concurrency and a 4 minute average call duration.

| Concurrent calls | Calls per month | Raw transcripts at 50 KB per call | With 2x amplification | Architecture action |
|---:|---:|---:|---:|---|
| 1K | 10.8M | 540 GB per month | 1.1 TB per month | Monthly partitions by `org_id` and time; archive recordings to object storage. |
| 10K | 108M | 5.4 TB per month | 10.8 TB per month | Read replicas, partition pruning, autovacuum tuning, object archival within 30 to 90 days. |
| 100K | 1.08B | 54 TB per month | 108 TB per month | Regional sharding, dedicated enterprise databases, cold storage, and analytics lake are mandatory. |

PostgreSQL remains the source of truth for relational entities such as `Organization`, `Call`, `Conversation`, `ConversationTurn`, `Ticket`, `Appointment`, and `ConsentRecord`. At 100K concurrency, transcripts and analytics should not live only in primary OLTP tables. Use tiered archival and keep hot operational windows short.

## 6.8 Redis sizing

| Tier | Live call state at 20 KB per call | Semantic and prompt cache | Recommended Redis shape | Notes |
|---:|---:|---:|---|---|
| 1K | 20 MB | 50 GB | 3 to 6 nodes, 12 vCPU, 48 GB RAM total | Cache dominates live state. |
| 10K | 200 MB | 200 GB | 6 to 12 nodes, 96 vCPU, 384 GB RAM total | Use tenant key prefixes and eviction policy by cache class. |
| 100K | 2 GB | 1 TB plus | 24 plus nodes, 512 vCPU, 2 TB RAM total | Redis is not a source of truth per D3; all state must be reconstructible. |

## 6.9 Total infrastructure estimate

| Tier | vCPU | RAM | Hot storage | Monthly new transcript storage | Network headroom | Notes |
|---:|---:|---:|---:|---:|---|
| 1K | 220 | 700 GB | 5 TB | 1.1 TB | 260 Mbps | One production region, three AZs, managed databases recommended. |
| 10K | 1,400 | 4.5 TB | 50 TB | 10.8 TB | 2.6 Gbps | Multi-region active-passive or active-active for enterprise tenants. |
| 100K | 12,000 | 32 TB | 500 TB plus | 108 TB | 26 Gbps | Multi-region fleet, tenant sharding, dedicated vendor capacity, and archival lake required. |

The estimates include application pods, Kafka, PostgreSQL, Redis, observability, and 25 to 35 percent operational headroom. They exclude vendor-side compute embedded in Twilio, STT, TTS, and LLM pricing.

## 6.10 Per-tier node-pool layout

| Tier | Node pool | Suggested nodes | Purpose |
|---:|---|---:|---|
| 1K | `rt-voice` | 8 x 8 vCPU 32 GB | `media-gateway-service`, `stt-adapter-service`, `tts-adapter-service`; tainted, Guaranteed QoS. |
| 1K | `ai-core` | 4 x 16 vCPU 64 GB | `ai-orchestrator-service`, `knowledge-service`. |
| 1K | `business-services` | 4 x 8 vCPU 32 GB | Tickets, scheduling, CRM, notification, campaign, analytics APIs. |
| 1K | `data-platform` | Managed preferred or 6 x 16 vCPU 64 GB | Kafka, Redis, PostgreSQL if self-managed. |
| 10K | `rt-voice` | 40 x 16 vCPU 64 GB | Hot-path speech and media pods. |
| 10K | `ai-core` | 20 x 32 vCPU 128 GB | AI orchestration, RAG, tool execution. |
| 10K | `business-services` | 20 x 16 vCPU 64 GB | Async and warm-path services. |
| 10K | `data-platform` | Managed preferred or 18 x 32 vCPU 128 GB | Kafka, Redis, PostgreSQL primary and replicas. |
| 100K | `rt-voice` | 300 x 16 vCPU 64 GB across regions | Media and speech edges, no single-cluster design. |
| 100K | `ai-core` | 120 x 32 vCPU 128 GB across regions | AI orchestration and knowledge service shards. |
| 100K | `business-services` | 100 x 16 vCPU 64 GB across regions | Async workflow and integration services. |
| 100K | `data-platform` | Managed regional fleets | Kafka shards, PostgreSQL shards, Redis clusters, observability storage. |

## 6.11 Cost critique for Twilio at scale

Decision D6 selects Twilio primary because it is the fastest path to market and has global PSTN reach. The standing critique is severe: at roughly USD 0.014 or more per minute, telephony becomes dominant at high concurrency.

| Tier | Call minutes per month | Twilio cost at USD 0.014 per min | Cost implication |
|---:|---:|---:|---|
| 1K | 43.2M | USD 604.8K per month | Already material; negotiate volume pricing. |
| 10K | 432M | USD 6.05M per month | SIP migration becomes economically necessary for high-volume tenants. |
| 100K | 4.32B | USD 60.48M per month | Twilio cannot be the only economic path; BYOC SIP and direct carrier contracts required. |

Alternatives are Amazon Connect, direct SIP trunking, carrier BYOC, or self-hosted media with LiveKit or Jambonz. Recommendation: use Twilio for speed and reliability in Phase 1, but build SIP trunking and tenant-level telephony abstraction before onboarding high-volume outbound or long-call tenants.

### Architecture Review (self-critique)

| Flaw or risk | Why it is a problem | Alternatives | Recommendation |
|---|---|---|---|
| Capacity tables imply precision that does not exist. | Real call duration, language, noise, model, and tool use can swing CPU and token demand by multiples. | Run synthetic and replay benchmarks; size per tenant; autoscale from live metrics. | Treat these numbers as order-of-magnitude planning and update after every load test. |
| 100K concurrency is vendor-quota limited. | LLM, STT, TTS, and telephony quotas may block scale even if Kubernetes is ready. | Dedicated capacity, multi-provider sharding, self-hosted components, traffic admission control. | Secure quotas before selling the tier; implement quota-aware routing and graceful degradation. |
| PostgreSQL transcript growth can overwhelm OLTP. | Hundreds of TB per month is not appropriate for hot relational storage. | Object storage data lake, partitioned cold tables, dedicated analytics warehouse. | Keep only hot operational windows in OLTP and archive transcripts plus recordings aggressively. |

# 7. Technology Evaluation

Scoring uses 1 low to 5 high. The selected option follows architecture contract decisions D1 through D6 unless a revisit trigger is met.

## 7.1 Kafka vs RabbitMQ

| Criteria | Kafka | RabbitMQ | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 5 | 3 | Kafka | Better for high-volume `conversation.turns.v1`, replay, analytics fan-out, and consumer groups. |
| Latency | 4 | 5 | RabbitMQ | RabbitMQ can be lower latency for command queues, but Kafka is not in the hot path. |
| Ops burden | 3 | 4 | RabbitMQ | Kafka KRaft, partitions, retention, and schema governance are heavier. |
| Spring ecosystem | 5 | 5 | Tie | Spring supports both well. |
| Cost | 3 | 4 | RabbitMQ | Kafka costs more below meaningful event volume. |
| Compliance and audit | 5 | 3 | Kafka | Replay and immutable log fit `audit.events.v1` and analytics requirements. |
| Overall | 25 | 24 | Kafka | Matches D1 for replay and high-throughput event streaming. |

Revisit trigger: fewer than 1K calls per day for a sustained period, no replay requirement, or Kafka operational incidents exceed team capacity. Recommendation: keep Kafka, preferably managed, and do not use it in the live voice loop.

## 7.2 PostgreSQL vs MongoDB

| Criteria | PostgreSQL 16 plus pgvector | MongoDB | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 4 | 4 | Tie | Both scale for operational workloads with correct partitioning or sharding. |
| Latency | 4 | 4 | Tie | PostgreSQL is predictable for indexed relational access; MongoDB is strong for document reads. |
| Ops burden | 4 | 3 | PostgreSQL | Fewer systems because relational data, RLS, JSONB, and pgvector are consolidated. |
| Spring ecosystem | 5 | 4 | PostgreSQL | Spring Data JPA, JDBC, Flyway, and R2DBC maturity. |
| Cost | 4 | 3 | PostgreSQL | Managed Postgres is broadly available and avoids separate vector or document stores early. |
| Compliance and audit | 5 | 3 | PostgreSQL | ACID, Row-Level Security, constraints, and transaction semantics fit multi-tenant records. |
| Overall | 26 | 21 | PostgreSQL | Matches D2 for relational domain, ACID, RLS, and pgvector consolidation. |

Revisit trigger: transcript volume overloads OLTP despite partitioning, tenant sharding becomes unmanageable, or document-heavy workflows dominate relational workflows. Recommendation: keep PostgreSQL as source of truth and move cold transcripts to object storage or analytics warehouses instead of replacing the core database.

## 7.3 Redis vs Hazelcast

| Criteria | Redis 7 | Hazelcast | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 5 | 4 | Redis | Excellent low-latency cache and rate-limit performance. |
| Latency | 5 | 4 | Redis | Mature sub-millisecond managed cache behavior. |
| Ops burden | 4 | 3 | Redis | More managed options and simpler operational model. |
| Spring ecosystem | 5 | 4 | Redis | Strong Spring Cache, Spring Data Redis, and rate-limit support. |
| Cost | 4 | 3 | Redis | Broad managed offerings and predictable cache sizing. |
| Compliance and audit | 3 | 3 | Tie | Neither should be the source of truth for compliance data. |
| Overall | 26 | 21 | Redis | Matches D3 for ubiquity, latency, ecosystem, and managed services. |

Revisit trigger: need distributed compute, near-cache data grids, or complex cluster-side processing that Redis does not fit. Recommendation: use Redis only for reconstructible state, semantic cache, rate limits, locks with care, and ephemeral call context.

## 7.4 Azure OpenAI vs OpenAI

| Criteria | Azure OpenAI | OpenAI | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 4 | 4 | Tie | Both can scale with quota negotiation; actual quota is account and region specific. |
| Latency | 4 | 5 | OpenAI | OpenAI may expose newer or faster models earlier. |
| Ops burden | 4 | 4 | Tie | Both are API services; Azure adds deployment and region management. |
| Spring ecosystem | 5 | 5 | Tie | Spring AI can abstract both through `ChatModel`. |
| Cost | 3 | 4 | OpenAI | Pricing changes frequently; Azure PTU may win at committed scale. |
| Compliance and audit | 5 | 3 | Azure OpenAI | Enterprise controls, private networking, data residency, SOC2, HIPAA BAA, no training on data. |
| Overall | 25 | 25 | Azure OpenAI primary | D5 prioritizes enterprise compliance over newest model access. |

Revisit trigger: Azure model lag blocks required capability, Azure regional quota cannot be obtained, or OpenAI offers compliant dedicated capacity meeting tenant residency. Recommendation: Azure OpenAI primary, OpenAI fallback only where policy allows, with tenant-specific fail-closed behavior.

## 7.5 Twilio vs Amazon Connect

| Criteria | Twilio | Amazon Connect | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 4 | 4 | Tie | Both can scale with enterprise planning and quotas. |
| Latency | 4 | 4 | Tie | Architecture and region placement dominate. |
| Ops burden | 4 | 3 | Twilio | Twilio is simpler for programmable voice and fast SIP or Media Streams integration. |
| Spring ecosystem | 4 | 3 | Twilio | Direct webhook and SDK model is straightforward. |
| Cost | 2 | 3 | Amazon Connect | Twilio at USD 0.014 plus per minute can dominate at scale. |
| Compliance and audit | 4 | 4 | Tie | Both support enterprise compliance patterns with correct configuration. |
| Overall | 22 | 21 | Twilio | Matches D6 for fastest time to market and global PSTN reach. |

Revisit trigger: telephony exceeds 35 percent of COGS, high-volume tenants demand BYOC, campaign scale exceeds Twilio economics, or regional coverage requires another carrier. Recommendation: Twilio primary in Phase 1, SIP and self-hosted media options in Phase 3 for high-volume tenants.

## 7.6 Pinecone vs pgvector

| Criteria | pgvector | Pinecone | Winner | Rationale |
|---|---:|---:|---|---|
| Throughput | 3 | 5 | Pinecone | Purpose-built vector service scales better at high vector counts. |
| Latency | 4 | 5 | Pinecone | Dedicated vector indexes can hold lower p95 at large scale. |
| Ops burden | 4 | 4 | Tie | pgvector avoids a system; Pinecone avoids index operations. |
| Spring ecosystem | 5 | 4 | pgvector | Easy PostgreSQL metadata joins and tenant filtering. |
| Cost | 5 | 3 | pgvector | Cheaper while vector volume is moderate and PostgreSQL is already required. |
| Compliance and audit | 5 | 4 | pgvector | RLS and transactional metadata are simpler in one database. |
| Overall | 26 | 25 | pgvector initially | Matches D4 because one fewer system and transactional consistency matter early. |

Revisit trigger: more than 20M vectors, retrieval p95 above 150 ms, HNSW maintenance hurts OLTP, or cross-tenant index isolation becomes hard. Recommendation: start with pgvector and evaluate Qdrant or Pinecone when D4 triggers occur.

### Architecture Review (self-critique)

| Flaw or risk | Why it is a problem | Alternatives | Recommendation |
|---|---|---|---|
| Scoring can hide business priorities. | A one-point score difference may not reflect compliance, team skills, or contract constraints. | Weighted scoring per tenant segment; proof-of-concept benchmarks; total cost modeling. | Use these scores as architectural defaults, not procurement decisions. |
| Vendor capabilities change quickly. | Model quality, telephony pricing, and vector DB performance evolve monthly. | Quarterly architecture review; contract exit clauses; adapter interfaces. | Keep abstractions thin but real, and define explicit revisit triggers as above. |
| Best technical option may not be best operational option. | Kafka, PostgreSQL, and Kubernetes require skilled 24x7 operations. | Managed services; simpler queue and database stack for small deployments. | Follow D1 to D6 for the platform, but use managed offerings until operating maturity is proven. |
