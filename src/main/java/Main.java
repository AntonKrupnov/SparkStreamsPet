import auto.ria.core.CarAvro;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
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
import java.util.function.Supplier;

public class Main {

    Supplier<Producer> producer = KafkaProducerUtils::newKafkaProducer;

    public static void main(String[] args) {
        KafkaProducerUtils.startProduceMessagesFromAvroFileToKafka();

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
                        ConsumerStrategies.Subscribe(ImmutableList.of(KafkaProducerUtils.MAIN_TOPIC), parameters));

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
                .reduceByKey((brand1, brand2) -> new CountTotalCost(
                        brand1.getCount() + brand2.getCount(),
                        brand1.getTotalCost() + brand2.getTotalCost()))
                .map(pair -> pair._1 + " -> " + pair._2.getCount() + ":" + pair._2.getTotalCost())
                .print();

        javaInputDStream
                .map(record -> (CarAvro) SerializationUtils.deserialize(record.value()))
                .mapToPair(carAvro ->
                        Tuple2.apply(
                                carAvro.getBrand() + ":" + carAvro.getModel() + ":" + carAvro.getYear(),
                                new CountTotalCostMinMax(1,
                                        carAvro.getPriceUsd(), carAvro.getPriceUsd(), carAvro.getPriceUsd())))
                .reduceByKey((model1, model2) ->
                        new CountTotalCostMinMax(
                                model1.count + model2.count,
                                model1.totalCost + model2.totalCost,
                                Math.min(model1.min, model2.min),
                                Math.max(model1.max, model2.max)))
                .map(pair -> pair._1 + " -> " +
                        pair._2.getCount() + ":" +
                        pair._2.getTotalCost() + ":" +
                        pair._2.getMin() + ":" +
                        pair._2.getMax())
                .print();

        javaStreamingContext.start();
    }

    private static class CountTotalCost implements Serializable {
        int count;
        double totalCost;

        CountTotalCost(int count, double totalCost) {
            this.count = count;
            this.totalCost = totalCost;
        }

        int getCount() {
            return count;
        }

        double getTotalCost() {
            return totalCost;
        }
    }

    private static class CountTotalCostMinMax implements Serializable {
        int count;
        double totalCost;
        double min;
        double max;

        CountTotalCostMinMax(int count, double totalCost, double min, double max) {
            this.count = count;
            this.totalCost = totalCost;
            this.min = min;
            this.max = max;
        }

        int getCount() {
            return count;
        }

        double getTotalCost() {
            return totalCost;
        }

        double getMin() {
            return min;
        }

        double getMax() {
            return max;
        }
    }

}
