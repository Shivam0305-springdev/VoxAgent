package com.shivam.aivoiceagent.ChatAgent.service;

import com.google.genai.Client;
import com.google.genai.ResponseStream;
import com.google.genai.types.Blob;
import com.google.genai.types.Candidate;
import com.google.genai.types.GenerateContentConfig;
import com.google.genai.types.GenerateContentResponse;
import com.google.genai.types.Part;
import com.google.genai.types.PrebuiltVoiceConfig;
import com.google.genai.types.SpeechConfig;
import com.google.genai.types.VoiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

/**
 * Text-to-speech via the Gemini TTS model.
 *
 * <p>Gemini returns raw PCM audio (24 kHz, 16-bit, mono) with no container, so
 * this service also handles WAV (RIFF) packaging: browsers and most players
 * refuse headerless PCM.
 */
@Service
public class GeminiTtsService {

    private static final Logger log = LoggerFactory.getLogger(GeminiTtsService.class);

    private static final String TTS_MODEL = "gemini-2.5-flash-preview-tts";
    private static final String VOICE_NAME = "Kore"; // Other options: Puck, Aoede, Fenrir, ...

    // PCM format Gemini TTS emits; must match the WAV header we generate
    private static final int SAMPLE_RATE = 24_000;
    private static final int CHANNELS = 1;
    private static final int BITS_PER_SAMPLE = 16;

    private final Client client;

    public GeminiTtsService(Client client) {
        this.client = client;
    }

    /**
     * Single-shot synthesis: returns the full utterance as a playable WAV file.
     * Blocks until the entire audio is generated.
     */
    public byte[] generateSpeech(String textToSpeak) {
        long start = System.currentTimeMillis();
        log.debug("TTS generateSpeech: {} chars, model={}", textToSpeak.length(), TTS_MODEL);

        GenerateContentResponse response = client.models.generateContent(TTS_MODEL, textToSpeak, ttsConfig());

        byte[] pcm = extractAudio(response)
                .orElseThrow(() -> new IllegalStateException("Could not retrieve a valid audio payload from Gemini"));

        log.debug("TTS generateSpeech done: {} PCM bytes in {} ms", pcm.length, System.currentTimeMillis() - start);
        return wrapPcmInWav(pcm);
    }

    /**
     * Writes the one-time WAV header for a live stream. The data length is
     * unknown up front, so the header advertises the maximum size — players
     * treat this as "read until the stream ends".
     */
    public void writeStreamingWavHeader(OutputStream outputStream) throws IOException {
        outputStream.write(wavHeader(0xFFFFFFFFL));
    }

    /**
     * Convenience for a self-contained live stream: WAV header followed by the
     * PCM fragments for a single text. For multi-sentence streams, write the
     * header once via {@link #writeStreamingWavHeader} and call
     * {@link #streamPcm} per sentence instead.
     */
    public void streamAudioChunks(String text, OutputStream outputStream) {
        try {
            writeStreamingWavHeader(outputStream);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed writing audio header to output stream", e);
        }
        streamPcm(text, outputStream);
    }

    /**
     * Streams headerless PCM fragments for the given text, flushing each chunk
     * as it arrives so the client can play audio while synthesis is still
     * running. Safe to call repeatedly on the same stream (raw PCM concatenates
     * cleanly).
     */
    public void streamPcm(String text, OutputStream outputStream) {
        String sentence = text.trim();
        if (sentence.isEmpty()) {
            return;
        }

        long start = System.currentTimeMillis();
        int chunkCount = 0;
        long totalBytes = 0;

        try (ResponseStream<GenerateContentResponse> audioChunks =
                     client.models.generateContentStream(TTS_MODEL, sentence, ttsConfig())) {
            for (GenerateContentResponse chunk : audioChunks) {
                // Chunks without inline audio (keep-alives, metadata frames) are skipped
                Optional<byte[]> audioBytes = extractAudio(chunk);
                if (audioBytes.isPresent()) {
                    if (chunkCount == 0) {
                        log.debug("TTS first audio chunk after {} ms", System.currentTimeMillis() - start);
                    }
                    chunkCount++;
                    totalBytes += audioBytes.get().length;
                    outputStream.write(audioBytes.get());
                    // Flush per chunk so the network layer forwards audio immediately
                    outputStream.flush();
                }
            }
            log.debug("TTS streamed {} chunk(s), {} bytes in {} ms for {} chars",
                    chunkCount, totalBytes, System.currentTimeMillis() - start, sentence.length());
        } catch (IOException e) {
            // Client likely disconnected mid-playback; propagate so the caller aborts the turn
            log.warn("Audio stream aborted after {} chunk(s): {}", chunkCount, e.getMessage());
            throw new UncheckedIOException("Failed writing audio chunk to output stream", e);
        }
    }

    /** Instructs Gemini to respond with audio only, using the configured prebuilt voice. */
    private GenerateContentConfig ttsConfig() {
        return GenerateContentConfig.builder()
                .responseModalities(List.of("AUDIO"))
                .speechConfig(SpeechConfig.builder()
                        .voiceConfig(VoiceConfig.builder()
                                .prebuiltVoiceConfig(PrebuiltVoiceConfig.builder()
                                        .voiceName(VOICE_NAME)
                                        .build())
                                .build())
                        .build())
                .build();
    }

    /**
     * Navigates the response tree (candidate → content → part → inline blob) to
     * the decoded PCM bytes. Every level is Optional because streaming responses
     * may contain structural frames without audio.
     */
    private Optional<byte[]> extractAudio(GenerateContentResponse response) {
        return response.candidates().stream()
                .flatMap(List::stream)
                .findFirst()
                .flatMap(Candidate::content)
                .flatMap(content -> content.parts().stream()
                        .flatMap(List::stream)
                        .findFirst())
                .flatMap(Part::inlineData)
                .flatMap(Blob::data);
    }

    /** Prepends a RIFF header sized to the actual PCM payload, producing a valid WAV file. */
    private static byte[] wrapPcmInWav(byte[] pcm) {
        byte[] header = wavHeader(pcm.length);
        byte[] wav = new byte[header.length + pcm.length];
        System.arraycopy(header, 0, wav, 0, header.length);
        System.arraycopy(pcm, 0, wav, header.length, pcm.length);
        return wav;
    }

    /**
     * Builds the standard 44-byte WAV (RIFF) header for the PCM format Gemini
     * emits. Sizes are clamped to 0xFFFFFFFF for streams of unknown length.
     */
    private static byte[] wavHeader(long dataLength) {
        int byteRate = SAMPLE_RATE * CHANNELS * BITS_PER_SAMPLE / 8;
        long riffSize = Math.min(dataLength + 36, 0xFFFFFFFFL);
        long dataSize = Math.min(dataLength, 0xFFFFFFFFL);

        return java.nio.ByteBuffer.allocate(44)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN)
                .put("RIFF".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                .putInt((int) riffSize)
                .put("WAVE".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                .put("fmt ".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                .putInt(16)                                          // fmt chunk size (PCM)
                .putShort((short) 1)                                 // audio format: 1 = linear PCM
                .putShort((short) CHANNELS)
                .putInt(SAMPLE_RATE)
                .putInt(byteRate)
                .putShort((short) (CHANNELS * BITS_PER_SAMPLE / 8)) // block align
                .putShort((short) BITS_PER_SAMPLE)
                .put("data".getBytes(java.nio.charset.StandardCharsets.US_ASCII))
                .putInt((int) dataSize)
                .array();
    }
}
