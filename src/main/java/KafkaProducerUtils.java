import auto.ria.core.CarAvro;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

class KafkaProducerUtils {

    static final String MAIN_TOPIC = "topic";
    static final String YEAR_TOPIC = "year";
    static final String MODEL_TOPIC = "model";
    static final String BRAND_TOPIC = "brand";
    private static final String BROKER_HOST = "localhost:29092";
    private static int i;

    static void startProduceMessagesFromAvroFileToKafka() {
        createTopicIfNotExists(KafkaProducerUtils.MAIN_TOPIC);

        Producer<String, byte[]> producer = newKafkaProducer();

        File file = new File("auto.ria.avro");
        DatumReader<CarAvro> reader = new SpecificDatumReader<>(CarAvro.class);
        DataFileReader<CarAvro> fileReader = null;
        try {
            fileReader = new DataFileReader<>(file, reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (fileReader != null) {
            for (int i = 0; fileReader.hasNext() && i < 20; i++) {
                System.out.println("Sending...");
                CarAvro carAvro = fileReader.next();
                byte[] datum = SerializationUtils.serialize(carAvro);
                producer.send(new ProducerRecord<>(MAIN_TOPIC,
                        carAvro.getId().toString(), datum));
                System.out.println("Sent: " + carAvro);
            }
        }
    }

    public static Producer<String, byte[]> newKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        return new KafkaProducer<>(properties);
    }

    public static Producer<String, String> newStringKafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer" + i++);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        return new KafkaProducer<>(properties);
    }

    public static void createTopicsIfNotExists() {
        createTopicIfNotExists(BRAND_TOPIC);
        createTopicIfNotExists(YEAR_TOPIC);
        createTopicIfNotExists(MODEL_TOPIC);
    }

    private static void createTopicIfNotExists(String topic) {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin");

        AdminClient adminClient = AdminClient.create(properties);
        try {
            if (!adminClient.listTopics().names().get().contains(topic)) {
                NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }
}
