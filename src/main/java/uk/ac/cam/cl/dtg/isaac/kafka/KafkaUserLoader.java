package uk.ac.cam.cl.dtg.isaac.kafka;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.serialization.StringSerializer;
import uk.ac.cam.cl.dtg.isaac.kafka.database.PostgresSqlDb;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by du220 on 02/08/2017.
 */
public class KafkaUserLoader {

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
                    "SELECT * FROM users"
            );

            ResultSet results = pst.executeQuery();

            /*while (results.next()) {

                try (Connection conn2 = postgresDB2.getDatabaseConnection()) {

                    pst = conn2.prepareStatement(
                            "INSERT INTO users(id, role, "
                                    + "gender, school_id, "
                                    + "school_other, email) "
                                    + "VALUES (?, ?, ?, ?, ?, ?);");

                    pst.setInt(1, results.getInt("id"));
                    pst.setString(2, results.getString("role"));
                    pst.setString(3, results.getString("gender"));
                    pst.setString(4, results.getString("school_id"));
                    pst.setString(5, results.getString("school_other"));
                    pst.setString(6, results.getString("email"));

                    pst.executeUpdate();


                } catch (SQLException ex) {
                    ex.printStackTrace();
                }

            }*/



            while (results.next()) {

                i++;
                ObjectNode record = JsonNodeFactory.instance.objectNode();

                record.put("given_name", NullHandler(results.getString("given_Name")));
                record.put("family_name", NullHandler(results.getString("family_name")));
                //record.put("email", NullHandler(results.getString("email")));
                record.put("user_id", NullHandler(results.getString("id")));
                record.put("role", NullHandler(results.getString("role")));
                record.put("date_of_birth", NullHandler(results.getString("date_of_birth")));
                record.put("gender", NullHandler(results.getString("gender")));
                record.put("registration_date", NullHandler(results.getDate("registration_date").getTime()));
                record.put("school_id", NullHandler(results.getString("school_id")));
                record.put("school_other", NullHandler(results.getString("school_other")));
                record.put("default_level ", NullHandler(results.getString("default_level")));
                //record.put("password", NullHandler(results.getString("password")));
                //record.put("secureSalt", NullHandler(results.getString("secure_salt")));
                //record.put("resetToken", NullHandler(results.getString("reset_token")));
                //record.put("resetExpiry", NullHandler(results.getString("reset_expiry")));

                //record.put("emailVerificationToken", NullHandler(results.getString("email_verification_token")));
                record.put("email_verification_status", NullHandler(results.getString("email_verification_status")));

                //record.put("last_updated", NullHandler(results.getString("last_updated")));
                //record.put("last_seen", NullHandler(results.getString("last_seen")));



                ObjectNode sqlRecord = JsonNodeFactory.instance.objectNode();
                sqlRecord.put("user_id", NullHandler(results.getString("id")));
                sqlRecord.put("anonymous_user", false);
                sqlRecord.put("event_type", "CREATE_UPDATE_USER");
                sqlRecord.put("event_details", record);
                sqlRecord.put("timestamp", NullHandler(results.getTimestamp("registration_date").getTime()));

                ProducerRecord<String, JsonNode> producerRecord = new ProducerRecord<String, JsonNode>("topic_logged_events", NullHandler(results.getString("id")),
                        sqlRecord);


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
