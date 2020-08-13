import auto.ria.core.CarAvro;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
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

class KafkaProducer {
    static final String MAIN_TOPIC = "topic";
    private static final String BROKER_HOST = "localhost:29092";

    static void startProduceMessagesFromAvroFileToKafka() {
        createTopicIfNotExists();

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);

        Producer<String, byte[]> producer = new org.apache.kafka.clients.producer.KafkaProducer<>(properties);
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

    private static void createTopicIfNotExists() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_HOST);
        properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "admin");

        AdminClient adminClient = AdminClient.create(properties);
        try {
            if (!adminClient.listTopics().names().get().contains(KafkaProducer.MAIN_TOPIC)) {
                NewTopic newTopic = new NewTopic(KafkaProducer.MAIN_TOPIC, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic));
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        adminClient.close();
    }
}
