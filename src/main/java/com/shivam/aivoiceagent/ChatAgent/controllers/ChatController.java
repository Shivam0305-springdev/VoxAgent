package com.shivam.aivoiceagent.ChatAgent.controllers;


import com.shivam.aivoiceagent.ChatAgent.service.ChatService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

/**
 * Voice-agent endpoints: both accept a text message and respond with
 * synthesized speech (WAV, 24 kHz 16-bit mono PCM).
 */
@RestController
@RequestMapping("/api/chat")
public class ChatController {

    private static final Logger log = LoggerFactory.getLogger(ChatController.class);

    private final ChatService chatService;

    public ChatController(ChatService chatService) {
        this.chatService = chatService;
    }

    /**
     * Blocking variant: returns the complete WAV file once fully generated.
     * Useful for testing/downloads; latency grows with response length.
     */
    @GetMapping
    public ResponseEntity<byte[]> chat(@RequestParam(value = "message") String message) {
        log.info("GET /api/chat");
        byte[] rawAudioBytes = chatService.chat(message);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.parseMediaType("audio/wav"));
        headers.setContentDispositionFormData("inline", "synthesized-speech.wav");

        return new ResponseEntity<>(rawAudioBytes, headers, HttpStatus.OK);
    }

    /**
     * Streaming variant: audio chunks are flushed as sentences are synthesized,
     * so playback starts long before the full response is ready.
     * StreamingResponseBody makes Spring use chunked transfer encoding.
     */
    @GetMapping("/synthesize")
    public ResponseEntity<StreamingResponseBody> interactiveChat(@RequestParam(value = "message") String message) {
        log.info("GET /api/chat/synthesize");
        return ResponseEntity.ok()
                .contentType(MediaType.parseMediaType("audio/wav"))
                .header(HttpHeaders.CONTENT_DISPOSITION, "inline")
                .body(chatService.interactiveChat(message));
    }
}
