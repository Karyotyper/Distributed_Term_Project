package flavors;

import java.util.Properties;
import java.util.Arrays;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

//JDBC
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

// Usage:\nFlavorConsumer <kafka-topic-name> <mapd-database-password>

public class FlavorConsumer {
    public static void main(String[] args) throws Exception {

        // Configure the Kafka Consumer
        String topicName = "flavor";
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092"); // Use 9097 so as not
        // to collide with
        // MapD Immerse
        props.put("group.id", "test");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // Subscribe the Kafka Consumer to the topic.
        consumer.subscribe(Arrays.asList(topicName));

        // print the topic name
        System.out.println("Subscribed to topic " + topicName);

        String flavorValue = "";

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);

                // STEP 4: Populate the prepared statement batch
                for (ConsumerRecord<String, String> record : records) {
                    flavorValue = record.value();

                    //print the value
                    System.out.println(flavorValue);

                    //analyse or process the data using Spark/Storm

//
                }
            System.out.println(records);
            }
    }
}


