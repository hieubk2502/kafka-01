package com.dev.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Map;

@Slf4j
public class BotCountStreamBuilder {

    private static final String BOT_COUNT_STORE = "bot-count-store";
    private static final String BOT_COUNT_TOPIC = "wikimedia.stats.bots";
    private ObjectMapper objectMapper = new ObjectMapper();


    private final KStream<String, String> inputStream;


    public BotCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        this.inputStream
                .mapValues(changeJson ->
                {
                    try {
                        JsonNode jsonNode = objectMapper.readTree(changeJson);

                        if (jsonNode.get("bot").asBoolean()) {
                            return "bot";
                        }
                        return "non-bot";
                    } catch (Exception e) {
                        return "parse-error";
                    }
                })
                .groupBy((key, botOrNot) -> botOrNot)
                .count(Materialized.as(BOT_COUNT_STORE))
                .toStream()
                .mapValues((k, v) -> {
                    Map<String, Long> kvMap = Map.of(String.valueOf(k), v);
                    try {
                        return objectMapper.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        log.error("Error when map Object to Json");
                        return null;
                    }

                })
                .to(BOT_COUNT_TOPIC);
    }
}
