package com.shivam.aivoiceagent.ChatAgent.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Orchestrates a voice-agent turn: sends the user's message to the configured
 * LLM (selected via {@code spring.ai.chat-model} in LLMConfig) and converts the
 * reply to speech through {@link GeminiTtsService}.
 */
@Service
public class ChatService {

    private static final Logger log = LoggerFactory.getLogger(ChatService.class);

    private final ChatClient chatClient;
    private final GeminiTtsService geminiTtsService;

    public ChatService(ChatClient chatClient, GeminiTtsService geminiTtsService) {
        this.chatClient = chatClient;
        this.geminiTtsService = geminiTtsService;
    }

    /**
     * Blocking, single-shot flow: waits for the complete LLM answer, then
     * synthesizes it as one WAV file. Simple, but time-to-first-audio is the
     * sum of full LLM + full TTS latency — use {@link #interactiveChat} for
     * live playback.
     */
    public byte[] chat(String message) {
        long start = System.currentTimeMillis();
        log.info("Chat request received ({} chars)", message.length());

        String llmResponse = chatClient.prompt()
                .user(message)
                .call()
                .content();

        long llmDone = System.currentTimeMillis();
        log.info("LLM response received in {} ms ({} chars)", llmDone - start,
                llmResponse == null ? 0 : llmResponse.length());

        byte[] audio = this.geminiTtsService.generateSpeech(llmResponse);

        log.info("TTS synthesis completed in {} ms ({} bytes), total turn {} ms",
                System.currentTimeMillis() - llmDone, audio.length, System.currentTimeMillis() - start);
        return audio;
    }

    /**
     * Low-latency streaming flow: consumes LLM tokens as they are generated and
     * hands each completed sentence to TTS immediately, so audio playback starts
     * after roughly one sentence of LLM latency instead of the full response.
     */
    public StreamingResponseBody interactiveChat(String message) {
        return outputStream -> {
            long start = System.currentTimeMillis();
            log.info("Interactive chat started ({} chars)", message.length());

            // WAV header must be written exactly once, before any PCM fragments
            geminiTtsService.writeStreamingWavHeader(outputStream);

            StringBuilder buffer = new StringBuilder();
            int sentenceCount = 0;

            Iterable<String> tokens = chatClient.prompt()
                    .user(message)
                    .stream()
                    .content()
                    .toIterable();

            for (String token : tokens) {
                buffer.append(token);

                // Speak at sentence boundaries: waiting for a full sentence gives the
                // TTS model enough context for natural intonation, while still
                // overlapping synthesis with ongoing LLM generation.
                if (token.contains(".") || token.contains("!") || token.contains("?")) {
                    sentenceCount++;
                    log.debug("Sentence {} complete after {} ms, sending to TTS: \"{}\"",
                            sentenceCount, System.currentTimeMillis() - start, buffer.toString().trim());
                    geminiTtsService.streamPcm(buffer.toString(), outputStream);
                    buffer.setLength(0);
                }
            }

            // The LLM may end without a terminal punctuation mark; speak the remainder
            geminiTtsService.streamPcm(buffer.toString(), outputStream);

            log.info("Interactive chat finished: {} sentence(s) in {} ms",
                    sentenceCount, System.currentTimeMillis() - start);
        };
    }
}
