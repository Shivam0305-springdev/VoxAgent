package com.shivam.aivoiceagent.ChatAgent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.google.genai.GoogleGenAiChatModel;
import org.springframework.ai.ollama.OllamaChatModel;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Selects the chat LLM provider at startup based on {@code spring.ai.chat-model}
 * (values: "ollama" or "gemini"). Exactly one ChatClient bean is created;
 * consumers stay provider-agnostic by injecting ChatClient.
 */
@Configuration
public class LLMConfig {

    private static final Logger log = LoggerFactory.getLogger(LLMConfig.class);

    // Applied to every conversation turn; keep it short — long prompts add latency and cost
    private static final String SYSTEM_PROMPT =
            "You are a helpful, concise call center agent. Keep answers very short.";

    @Bean
    @ConditionalOnProperty(prefix = "spring.ai", name = "chat-model", havingValue = "ollama")
    ChatClient ollamaChatModel(OllamaChatModel ollamaChatModel) {
        log.info("Chat provider: Ollama (spring.ai.chat-model=ollama)");
        return ChatClient.builder(ollamaChatModel)
                .defaultSystem(SYSTEM_PROMPT)
                .build();
    }


    @Bean
    @ConditionalOnProperty(prefix = "spring.ai", name = "chat-model", havingValue = "gemini")
    ChatClient geminiChatModel(GoogleGenAiChatModel googleGenAiChatModel) {
        log.info("Chat provider: Google Gemini (spring.ai.chat-model=gemini)");
        return ChatClient.builder(googleGenAiChatModel)
                .defaultSystem(SYSTEM_PROMPT)
                .build();
    }
}
