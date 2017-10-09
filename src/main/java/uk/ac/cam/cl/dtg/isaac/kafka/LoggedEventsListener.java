/**
 * Copyright 2017 Dan Underwood
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 * 		http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.cam.cl.dtg.isaac.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KeyValue;
import uk.ac.cam.cl.dtg.isaac.kafka.customAggregators.QuestionAnswerCounter;
import uk.ac.cam.cl.dtg.isaac.kafka.customAggregators.QuestionAnswerInitializer;
import uk.ac.cam.cl.dtg.isaac.kafka.customProcessors.ThresholdAchievedProcessor;
import uk.ac.cam.cl.dtg.isaac.kafka.database.PostgresSqlDb;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.JsonNode;
import uk.ac.cam.cl.dtg.isaac.kafka.utilities.DerivedStreams;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.*;


/**
 *  Kafka streams processing for Isaac user achievements
 *  @author Dan Underwood
 */
public class LoggedEventsListener {

    final static Serializer<JsonNode> jsonSerializer = new JsonSerializer();
    final static Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
    final static Serde<JsonNode> JsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    final static Serde<String> StringSerde = Serdes.String();
    final static Serde<Long> LongSerde = Serdes.Long();
    static ThresholdAchievedProcessor achievementProcessor;

    // config variables
    public static Properties configVariables;

    public static void main(String[] args) throws IOException {

        KStreamBuilder builder = new KStreamBuilder();
        Properties streamsConfiguration = new Properties();



        // set up kafka streaming variables
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "loggedEventListener");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 10 * 1000);
        streamsConfiguration.put(StreamsConfig.consumerPrefix(ConsumerConfig.METADATA_MAX_AGE_CONFIG), 60 * 1000);
        streamsConfiguration.put(StreamsConfig.producerPrefix(ProducerConfig.METADATA_MAX_AGE_CONFIG), 60 * 1000);
        //streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        // raw logged_events incoming data stream from kafka
        KStream<String, JsonNode>[] rawLoggedEvents = builder.stream(StringSerde, JsonSerde, "topic_logged_events")
                .branch(
                        (k, v) -> !v.path("anonymous_user").asBoolean(),
                        (k, v) -> v.path("anonymous_user").asBoolean()
                );

        // parallel log for anonymous events (may want to optimise how we do this later)
        rawLoggedEvents[1].to(StringSerde, JsonSerde, "topic_anonymous_logged_events");


        // SITE STATISTICS
        KTable<String, JsonNode> userData = DerivedStreams.FilterByEventType(rawLoggedEvents[0], "CREATE_UPDATE_USER")
                // set up a local user data store
                .groupByKey(StringSerde, JsonSerde)
                .aggregate(
                        // initializer
                        () -> {
                            ObjectNode userRecord = JsonNodeFactory.instance.objectNode();

                            userRecord.put("user_id", "");
                            userRecord.put("user_data", "");

                            return userRecord;
                        },
                        // aggregator
                        (userId, userUpdateLogEvent, userRecord) -> {

                            ((ObjectNode) userRecord).put("user_id", userId);
                            ((ObjectNode) userRecord).put("user_data", userUpdateLogEvent.path("event_details"));

                            return userRecord;
                        },
                        JsonSerde,
                        "store_user_data"
                );

        // join user table to incoming event stream to get user data for stats processing
        KStream<String, JsonNode> userEvents = rawLoggedEvents[0]

                .join(
                        userData,
                        (logEventVal, userDataVal) -> {
                            ObjectNode joinedValueRecord = JsonNodeFactory.instance.objectNode();

                            joinedValueRecord.put("user_id", userDataVal.path("user_data").path("user_id"));
                            joinedValueRecord.put("user_role", userDataVal.path("user_data").path("role"));
                            joinedValueRecord.put("user_gender", userDataVal.path("user_data").path("gender"));
                            joinedValueRecord.put("event_type", logEventVal.path("event_type"));
                            joinedValueRecord.put("event_details", logEventVal.path("event_details"));
                            joinedValueRecord.put("timestamp", logEventVal.path("timestamp"));

                            return joinedValueRecord;
                        }
                );


        // maintain internal store of users' last seen times, by log event type
        userEvents
                .groupByKey(StringSerde, JsonSerde)
                .aggregate(
                        // initializer
                        () -> {
                            ObjectNode countRecord = JsonNodeFactory.instance.objectNode();
                            countRecord.put("OVERALL", 0);
                            return countRecord;
                        },
                        // aggregator
                        (userId, logEvent, countRecord) -> {

                            String eventType = logEvent.path("event_type").asText();
                            Timestamp stamp = new Timestamp(logEvent.path("timestamp").asLong());

                            ((ObjectNode) countRecord).put(eventType, stamp.getTime());
                            ((ObjectNode) countRecord).put("OVERALL", stamp.getTime());

                            return countRecord;
                        },
                        JsonSerde,
                        "store_user_last_seen"
                );



        // maintain internal store of log event type counts
        userEvents
                .map(
                        (k, v) -> {
                            return new KeyValue<String, JsonNode>(v.path("event_type").asText(), v);
                        }
                )
                .groupByKey(StringSerde, JsonSerde)
                .count("store_log_event_counts");



        // maintain internal store of log event counts per user type, per day
        userEvents
                .map(
                        (k, v) -> {

                            Timestamp stamp = new Timestamp(v.path("timestamp").asLong());
                            Calendar cal = Calendar.getInstance();
                            cal.setTime(stamp);
                            cal.set(Calendar.HOUR_OF_DAY, 0);
                            cal.set(Calendar.MINUTE, 0);
                            cal.set(Calendar.SECOND, 0);
                            cal.set(Calendar.MILLISECOND, 0);

                            return new KeyValue<Long, JsonNode>(cal.getTimeInMillis(), v);
                        }
                )
                .groupByKey(LongSerde, JsonSerde)
                .aggregate(
                        // initializer
                        () -> {
                            ObjectNode countRecord = JsonNodeFactory.instance.objectNode();
                            return countRecord;
                        },
                        // aggregator
                        (userId, logEvent, countRecord) -> {

                            String userRole = logEvent.path("user_role").asText();

                            if (!countRecord.has(userRole)) {
                                ObjectNode logEventTypes = JsonNodeFactory.instance.objectNode();
                                ((ObjectNode) countRecord).put(userRole, logEventTypes);
                            }

                            String eventType = logEvent.path("event_type").asText();

                            if (countRecord.path(userRole).has(eventType)) {

                                Long count = countRecord.path(userRole).path(eventType).asLong();
                                ((ObjectNode) countRecord.path(userRole)).put(eventType, count + 1);

                            } else {
                                ((ObjectNode) countRecord.path(userRole)).put(eventType, 1);
                            }

                            return countRecord;
                        },
                        JsonSerde,
                        "store_daily_log_events"
                );



        //use the builder and the streamsConfiguration we set to setup and start a streams object
        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.cleanUp();
        streams.start();

        //shutdown on an interrupt
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}





