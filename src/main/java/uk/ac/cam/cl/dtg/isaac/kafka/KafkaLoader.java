package uk.ac.cam.cl.dtg.isaac.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import uk.ac.cam.cl.dtg.isaac.kafka.database.PostgresSqlDb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by du220 on 06/06/2017.
 */
public class KafkaLoader {

    public static void main(String[] args) throws IOException {

        // config variables
        Properties configVariables = new Properties();

        InputStream ioStream = null;
        PostgresSqlDb postgresDB = null;
        PostgresSqlDb postgresDB2 = null;

        ObjectMapper objectMapper = new ObjectMapper();
        JsonNodeFactory nodeFactory = JsonNodeFactory.instance;
        JsonFactory fac = objectMapper.getFactory();

        // set up config variables
        try {
            ioStream = ClassLoader.getSystemResourceAsStream("config/config.properties");
            configVariables.load(ioStream);

        } catch (IOException ex) {
            ex.printStackTrace();
        }


        Properties kafkaProps = new Properties();
        kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                configVariables.get("KAFKA_HOSTNAME").toString() + ":" + configVariables.get("KAFKA_PORT").toString());
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.connect.json.JsonSerializer.class);


        // set up postgres DB instance
        try {
            postgresDB = new PostgresSqlDb(configVariables.get("POSTGRES_DB_URL").toString(),
                    configVariables.get("POSTGRES_DB_USER").toString(),
                    configVariables.get("POSTGRES_DB_PASSWORD").toString());


            /*postgresDB2 = new PostgresSqlDb("jdbc:postgresql://localhost/rutherford",
                    "rutherford",
                    "rutherf0rd");*/


        } catch (ClassNotFoundException e) {
            System.out.println("Unable to locate postgres driver: " + e);
        }


        KafkaProducer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(kafkaProps);

        PreparedStatement pst;
        try (Connection conn = postgresDB.getDatabaseConnection()) {

            int i = 0;

            pst = conn.prepareStatement(
                    "select * from logged_events " +
                            "where timestamp > '2015-01-01 00:00:00' " +
                            "and timestamp < '2016-01-01 00:00:00' "
            );

            ResultSet results = pst.executeQuery();

            System.out.println("Query executed!");

            while (results.next()) {

                i++;
                JsonNode eventDetails = objectMapper.readTree(results.getString("event_details").replace("%", "%%"));

                ObjectNode record = JsonNodeFactory.instance.objectNode();

                record.put("user_id", NullHandler(results.getString("user_id")));
                record.put("anonymous_user", NullHandler(results.getBoolean("anonymous_user")));
                record.put("event_type", NullHandler(results.getString("event_type")));
                record.put("event_details_type", NullHandler(results.getString("event_details_type")));
                record.put("event_details", eventDetails);
                record.put("ip_address", results.getString("ip_address"));
                record.put("timestamp", results.getTimestamp("timestamp").getTime());

                ProducerRecord producerRecord = new ProducerRecord<String, JsonNode>("topic_logged_events", record.get("user_id").asText(),
                        record);

                try {
                    producer.send(producerRecord);
                    Thread.sleep(1);
                } catch (KafkaException kex) {
                    kex.printStackTrace();
                } catch (InterruptedException e) {

                }

            }


        } catch(SQLException ex){
            ex.printStackTrace();
        }
    }

    public static String NullHandler(Object value) {

        return value!=null ? value.toString() : "";
    }

}