package com.dev;

import com.google.gson.JsonParser;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
//        String uriLocal = "http://localhost:9200";

        String uriBonsai = "https://fk6nv0rcuo:42v63283x1@srv-search-1044945645.us-east-1.bonsaisearch.net:443";

        // build URI from the connection string
        RestHighLevelClient restHighLevelClient;

        URI connectUri = URI.create(uriBonsai);

        // extract login information if it exists
        String userInfo = connectUri.getUserInfo();

        if (userInfo != null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connectUri.getHost(), connectUri.getPort(), connectUri.getScheme())));
        } else {
            // Rest client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connectUri.getHost(), connectUri.getPort(), connectUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())
                            )
            );
        }
        return restHighLevelClient;
    }

    private static void addShutdownHook(KafkaConsumer<String, String> consumer) {
        final Thread threadCurrent = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer consumer.wakeup()...");

                // allow consumer to gracefull exit the polling loop and stip
                consumer.wakeup();

                try {
                    threadCurrent.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {
        log.info("I am Consumer");
        String boostrapServers = "127.0.0.1:9092";
        String groupId = "consumer_opensearch";

        // create Producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return new KafkaConsumer<>(properties);
    }

    @SneakyThrows
    public static void main(String[] args) {
        // create on OpenSearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        // create kafka client
        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(Arrays.asList("wikimedia.recentchange"));
        addShutdownHook(kafkaConsumer);
        // create index on openSearch if doesn't exist already
        try {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("The wikimedia index has been created");
            } else {
                log.info("The wikimedia index already exists");
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
                log.info("Received " + records.count() + " records");

                BulkRequest bulkRequest = new BulkRequest();
                for (ConsumerRecord<String, String> record : records) {
                    // sent to opensearch
                    String id = "";

                    // strategy 1
                    // define on ID using kafka Record coordinates
//              id = record.topic()+"_" + record.partition() + "_" + record.offset();

                    try {
                        // strategy 2
                        // exact id from body value

                        id = extractId(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        bulkRequest.add(indexRequest);
//                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
//                    log.info("Inserted with id: " + indexResponse.getId());
                    } catch (Exception e) {
                        log.error("Error insert: {}", id);
                    }
                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkResponse = null;
                    try {
                        bulkResponse = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    log.info("Insert " + bulkResponse.getItems().length + " records");

                    Thread.sleep(5000);
                }

                kafkaConsumer.commitSync();
                log.info("Commited offset");
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shutdown");
        } catch (Exception e) {
            log.error("Error {}", e);
        } finally {
            kafkaConsumer.close();
            try {
                openSearchClient.close();
            } catch (IOException e) {
                log.error("Error {}", e);
            }
        }

    }

    private static String extractId(String body) {
        // gson
        return JsonParser.parseString(body)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }
}

// Delivery semantics for consumers Summary
// At most one : offset are committed as soon as the message is received. if the processing goes wrong, the message will lost ( it won't be read again)
// at least one ( preferred) : offset are committed after the message is processed. if the processing goes wrong, message will be read again.
//                              the result in duplicate processing of message. Make sure your processing is idempotent (processing again message won't impact your systems)]
// Exactly once: can be achieved for kafka. kafka workflows using TRANSACTIONAL API. -> USE IDEMPOTENT CONSUMER.

// Consumer coordinator (acting broker) - register consumer-group-application
// COnsumers in a group talk to a consumer groups coordinator
// to detect consumers that are down, the is a heart beat mechanism and a pool mechanism
// send heartbeat (thread) to COnsumer coordinator
// poll (thread) from broker
// heartbeat.interval.ms , session.timout.ms
// base on send heartbeat lower or faster to rebalences.

// max.poll.interval.ms (5 minutes)
// maximum amount of time between two .poll() call before declaring the consumer deal
// this i relevant for Big data frameworks like spark in case the processing takes time
// max.poll.records

// fetch.min.bytes : controls how much data you want pull at least on each request
// improve throughput and decreasing request number
// cost of latency

// fetch.max.wait.ms
//the maximum amount of time kafka broker block befor answering the fetch request.

// max partition fetch bytes
// maximum amount of data per partition server will return
// example: set 5KB. if have 100 partition return , 1 partition only return 5KB

// fetch.max.bytes
// maximum data all partition server will return

