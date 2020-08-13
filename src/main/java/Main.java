import auto.ria.core.CarAvro;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        KafkaProducer.startProduceMessagesFromAvroFileToKafka();

        SparkConf sparkConf = new SparkConf().setAppName("sparkStreamingPet").setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

        Map<String, Object> parameters = new HashMap<>();
        parameters.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        parameters.put(ConsumerConfig.GROUP_ID_CONFIG, "sparkStreaming");
        parameters.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        parameters.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        parameters.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        JavaInputDStream<ConsumerRecord<String, byte[]>> javaInputDStream =
                KafkaUtils.createDirectStream(javaStreamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(ImmutableList.of(KafkaProducer.MAIN_TOPIC), parameters));

        System.out.println("Printing...");
//        javaInputDStream
//                .foreachRDD((consumerRecordJavaRDD, time) ->
//                        consumerRecordJavaRDD.foreach(System.out::println));

        javaInputDStream.map(record -> SerializationUtils.deserialize(record.value())).print();

        javaStreamingContext.start();
    }

}
