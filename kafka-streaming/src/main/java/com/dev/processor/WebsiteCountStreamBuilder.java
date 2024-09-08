package com.dev.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;

@Slf4j
public class WebsiteCountStreamBuilder {

    private static final String WEBSITE_COUNT_STORE = "website-count-store";
    private static final String WEBSITE_COUNT_TOPIC = "wikimedia.stats.website";
    private ObjectMapper objectMapper = new ObjectMapper();


    private final KStream<String, String> inputStream;


    public WebsiteCountStreamBuilder(KStream<String, String> inputStream) {
        this.inputStream = inputStream;
    }

    public void setup() {
        final TimeWindows timeWindows = TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1L));
        this.inputStream
                .selectKey((k, changeJson) -> {
                    try {
                        final JsonNode jsonNode = objectMapper.readTree(changeJson);
                        return jsonNode.get("server_name").asText();
                    } catch (Exception e) {
                        return "parse-error";
                    }
                })
                .groupByKey()
                .windowedBy(timeWindows)
                .count(Materialized.as(WEBSITE_COUNT_STORE))
                .toStream()
                .mapValues((key, value) -> {
                    final Map<String, Object> kvMap = Map.of(
                            "website", key.key(),
                            "count", value
                    );
                    try {
                        return objectMapper.writeValueAsString(kvMap);
                    } catch (JsonProcessingException e) {
                        return null;
                    }
                })
                .to(WEBSITE_COUNT_TOPIC, Produced.with(
                        WindowedSerdes.timeWindowedSerdeFrom(String.class, timeWindows.size()),
                        Serdes.String()
                ));
    }
}
