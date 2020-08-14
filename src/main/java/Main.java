import auto.ria.core.CarAvro;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
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

//        javaInputDStream
//                .map(record -> SerializationUtils.deserialize(record.value()))
//                .print();

        javaInputDStream
                .map(record -> (CarAvro) SerializationUtils.deserialize(record.value()))
                .map(carAvro -> carAvro.getYear() + " -> " + carAvro.getPriceUsd())
                .print();

        javaInputDStream
                .map(record -> (CarAvro) SerializationUtils.deserialize(record.value()))
                .mapToPair(carAvro ->
                        Tuple2.apply(
                                carAvro.getBrand() + ":" + carAvro.getYear(),
                                new CountTotalCost(1, carAvro.getPriceUsd())))
                .reduceByKey((obj1, obj2) -> new CountTotalCost(
                        obj1.getCount() + obj2.getCount(),
                        obj1.getTotalCost() + obj2.getTotalCost()))
                .map(pair -> pair._1 + " -> " + pair._2.getCount() + ":" + pair._2.getTotalCost())
                .print();

        javaStreamingContext.start();
    }

    private static class CountTotalCost implements Serializable {
        int count;
        double totalCost;

        public CountTotalCost(int count, double totalCost) {
            this.count = count;
            this.totalCost = totalCost;
        }

        public int getCount() {
            return count;
        }

        public double getTotalCost() {
            return totalCost;
        }
    }

}
