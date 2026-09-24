# 23. Sequence Diagrams

Legend: solid arrows are synchronous REST, gRPC, or WebSocket interactions; dashed arrows are asynchronous Kafka publication or consumption. Service names use the canonical architecture contract exactly.

## 23.1 Incoming Call

```mermaid
sequenceDiagram
    autonumber
    participant Twilio
    participant CM as call-management-service
    participant CFG as config-service
    participant MG as media-gateway-service
    participant TTS as tts-adapter-service
    participant STT as stt-adapter-service
    participant AI as ai-orchestrator-service
    participant Kafka
    participant CONV as conversation-service
    participant AN as analytics-service
    participant CRM as crm-integration-service

    Twilio->>CM: POST voice webhook
    CM->>CM: validate Twilio signature
    CM->>CFG: REST get agent config
    CFG-->>CM: config and prompt version
    CM->>Twilio: TwiML stream URL
    Twilio->>MG: WebSocket media stream
    par greeting setup
        MG->>TTS: gRPC synthesize greeting
        TTS-->>MG: streaming audio
    and vendor warmup
        MG->>STT: gRPC open stream
        AI->>TTS: prewarm voice channel
    end
    MG->>Twilio: greeting audio
    loop turn loop p95 target 1500 ms
        Twilio->>MG: audio frames
        MG->>STT: gRPC audio frames
        STT-->>AI: partial and final transcript
        AI->>AI: guardrails and tool decision
        AI->>TTS: gRPC sentence chunks
        TTS-->>MG: first audio p50 800 ms
        MG->>Twilio: audio frames with barge in
        AI-->>Kafka: conversation.turns.v1
    end
    Twilio->>CM: POST status completed
    CM-->>Kafka: call.events.v1
    Kafka-->>CONV: call and turn projection
    Kafka-->>AN: metrics and disposition
    Kafka-->>CRM: call activity sync
```

Latency notes: webhook setup is control-plane and should complete within 500 ms. The live turn loop is the critical budget: VAD and endpointing 150 to 250 ms, STT final partial 100 to 200 ms, LLM TTFT 250 to 450 ms, TTS TTFB 100 to 200 ms, and jitter egress 50 to 100 ms.

### Architecture Review (self-critique)

The diagram is intentionally optimistic. Real Twilio webhook retries, NAT traversal, noisy mobile audio, and vendor WebSocket cold starts can erase the latency budget. The design must prewarm Deepgram and TTS streams at call start and aggressively measure p95 by stage, not just end-to-end averages.

## 23.2 Outbound Call

```mermaid
sequenceDiagram
    autonumber
    participant CAMP as campaign-service
    participant Kafka
    participant CM as call-management-service
    participant Twilio
    participant MG as media-gateway-service
    participant CFG as config-service
    participant AI as ai-orchestrator-service
    participant STT as stt-adapter-service
    participant TTS as tts-adapter-service
    participant AN as analytics-service

    CAMP->>CAMP: pacing and quiet hours
    CAMP-->>Kafka: campaign.dial.commands.v1
    Kafka-->>CM: dial command
    CM->>Twilio: REST originate call
    Twilio-->>CM: call sid accepted
    Twilio->>CM: status ringing
    Twilio->>CM: status answered with AMD result
    CM->>CFG: REST get campaign agent config
    CFG-->>CM: active config
    CM->>Twilio: connect stream TwiML
    Twilio->>MG: WebSocket media stream
    MG->>AI: gRPC start conversation
    loop conversation
        MG->>STT: gRPC audio
        STT-->>AI: transcript
        AI->>TTS: gRPC response
        TTS-->>MG: audio stream
        AI-->>Kafka: conversation.turns.v1
    end
    CM-->>Kafka: call.events.v1
    Kafka-->>AN: campaign outcome analytics
```

Latency notes: outbound answer detection is not in the voice-to-voice budget but affects abandonment and customer experience. Once answered, the same 800 ms p50 and 1500 ms p95 turn budget applies.

### Architecture Review (self-critique)

Outbound is a compliance minefield. The technical sequence is straightforward, but TCPA, consent, quiet hours, AMD false positives, and abandonment-rate rules are harder than dialing. `campaign-service` should not be shipped until consent records and audit events are enforceable.

## 23.3 Ticket Creation

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant MG as media-gateway-service
    participant STT as stt-adapter-service
    participant AI as ai-orchestrator-service
    participant TKT as ticket-service
    participant Kafka
    participant NOTIF as notification-service
    participant CRM as crm-integration-service
    participant TTS as tts-adapter-service

    Caller->>MG: request support ticket
    MG->>STT: gRPC audio
    STT-->>AI: intent transcript
    AI->>AI: validate tool policy
    AI->>TKT: gRPC create ticket timeout 800 ms
    alt create succeeds within timeout
        TKT->>TKT: idempotent create by actionId
        TKT-->>AI: ticket number
        TKT-->>Kafka: ticket.events.v1
        AI->>TTS: confirm ticket number
        TTS-->>MG: confirmation audio
        MG-->>Caller: ticket confirmation
    else create times out
        AI-->>Kafka: ai.actions.v1
        AI->>TTS: fallback SMS confirmation phrase
        TTS-->>MG: fallback audio
        MG-->>Caller: confirmation will arrive by SMS
        Kafka-->>TKT: async create by actionId
        TKT-->>Kafka: ticket.events.v1
    end
    Kafka-->>NOTIF: ticket notification
    Kafka-->>CRM: crm.sync.commands.v1
```

Resolution of tension: the agent attempts synchronous creation with an 800 ms deadline because callers expect a ticket number during the conversation. If the deadline fails, `ai-orchestrator-service` publishes `ai.actions.v1` through an outbox and uses a truthful fallback: the caller will receive SMS confirmation. `ticket-service` must be idempotent on `actionId` so sync and async paths cannot create duplicates.

### Architecture Review (self-critique)

This is the right compromise but still risky: synchronous business tools consume the LLM latency budget and introduce tail risk from PostgreSQL or CRM-like hooks. `ticket-service` must keep creation local, emit `ticket.events.v1`, and never block ticket number generation on external CRM synchronization.

## 23.4 Appointment Booking

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant MG as media-gateway-service
    participant AI as ai-orchestrator-service
    participant SCHED as scheduling-service
    participant Redis
    participant Kafka
    participant NOTIF as notification-service
    participant TTS as tts-adapter-service

    Caller->>MG: asks for appointment
    MG->>AI: gRPC turn intent
    AI->>SCHED: REST availability check
    SCHED->>Redis: read cached availability
    Redis-->>SCHED: slots
    SCHED->>Redis: hold slot with TTL
    SCHED-->>AI: slot options and hold id
    AI->>TTS: speak slot options
    TTS-->>MG: audio options
    Caller->>MG: confirms slot
    MG->>AI: gRPC confirmation
    AI->>SCHED: REST book idempotent
    SCHED->>Redis: validate hold TTL
    SCHED->>SCHED: commit appointment
    SCHED-->>AI: confirmation code
    SCHED-->>Kafka: appointment.events.v1
    AI->>TTS: speak confirmation
    TTS-->>MG: confirmation audio
    Kafka-->>NOTIF: send appointment SMS
```

Latency notes: availability lookup is a warm-path call and should target p95 below 300 ms from cache. Booking can exceed the voice loop budget if the caller is given a filler acknowledgement before confirmation.

### Architecture Review (self-critique)

Redis slot holds are necessary for conversational UX but insufficient for correctness. The final PostgreSQL write needs exclusion constraints or equivalent conflict detection. Calendar integrations can invalidate cached slots; the product must clearly state whether confirmation is final or pending external calendar acceptance.

## 23.5 Human Escalation

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant MG as media-gateway-service
    participant AI as ai-orchestrator-service
    participant AR as agent-routing-service
    participant Console as Human agent console
    participant Twilio
    participant Kafka
    participant AN as analytics-service
    participant NOTIF as notification-service

    Caller->>MG: asks for human help
    MG->>AI: gRPC escalation trigger
    AI->>AI: sentiment and guardrail check
    AI->>AR: REST queue lookup
    AR-->>AI: queue and wait time
    AI->>AR: REST create handoff
    AR->>Console: push context package
    Console-->>AR: agent accepts
    AR->>Twilio: warm transfer conference
    Twilio-->>Caller: join human agent
    AI-->>Kafka: escalation.events.v1
    AR-->>Kafka: escalation.events.v1
    Kafka-->>AN: escalation analytics
    Kafka-->>NOTIF: optional escalation notice
```

Context package: summary, transcript so far, detected intent, sentiment, identity verification status, tool actions, and recommended next step. It must be pushed before or at the same time as the conference invite to avoid a blind transfer.

### Architecture Review (self-critique)

The contract names `agent-routing-service`, but a production-grade human handoff needs a full console application, presence management, supervisor controls, and call disposition workflows. Without these, warm transfer is a telephony feature rather than a support product.

## 23.6 Knowledge Retrieval

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant MG as media-gateway-service
    participant STT as stt-adapter-service
    participant AI as ai-orchestrator-service
    participant Redis
    participant KNOW as knowledge-service
    participant PG as PostgreSQL pgvector
    participant TTS as tts-adapter-service
    participant Kafka
    participant CONV as conversation-service

    Caller->>MG: asks knowledge question
    MG->>STT: gRPC audio stream
    STT-->>AI: transcript final partial
    AI->>Redis: semantic cache check
    alt cache hit
        Redis-->>AI: grounded answer and citations
    else cache miss
        AI->>KNOW: gRPC hybrid search
        KNOW->>PG: vector and keyword search
        PG-->>KNOW: candidate chunks
        KNOW->>KNOW: rerank and filter by org
        KNOW-->>AI: top passages with citations
        AI->>Redis: cache answer candidate
    end
    AI->>AI: generate grounded answer
    AI->>TTS: stream sentence chunks
    TTS-->>MG: audio stream
    AI-->>Kafka: conversation.turns.v1
    Kafka-->>CONV: persist cited turn
```

Latency notes: semantic cache hit should return in under 50 ms. Cache miss retrieval should target p95 below 150 ms for pgvector under D4; crossing that threshold at scale is a trigger to evaluate Qdrant or Pinecone.

### Architecture Review (self-critique)

Grounding quality is not guaranteed by retrieval diagrams. The system needs eval sets, citation faithfulness scoring, chunk freshness checks, and refusal behavior when retrieved evidence is weak. Without that, fast RAG becomes fast hallucination with citations.
