package parser.rddloader;

import model.CityModel;
import model.WeatherDescriptionPojo;
import model.WeatherMeasurementPojo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;
import parser.description.WeatherDescriptionParserFlatMap;
import parser.measurement.WeatherMeasurementParserFlatMap;
import parser.validators.IMeasurementValidator;
import utils.configuration.AppConfiguration;
import utils.spark.SparkContextSingleton;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class WeatherRDDLoaderFromKafka {

    private static final String kafkaUri = AppConfiguration.getProperty("kafka.broker.uri");
    private JavaSparkContext jsc;
    private Map<String, CityModel> cities;

    private Map<String, Object> kafkaParamsForRDD;

    private KafkaConsumer<String, String> kafkaConsumer;



    private void initKafka() {
        //For createRDD
        kafkaParamsForRDD= new HashMap<>();
        kafkaParamsForRDD.put("bootstrap.servers", kafkaUri);
        kafkaParamsForRDD.put("key.deserializer", StringDeserializer.class);
        kafkaParamsForRDD.put("value.deserializer", StringDeserializer.class);
        kafkaParamsForRDD.put("group.id", "RDD_LOADER_FROM_KAFKA");
        kafkaParamsForRDD.put("auto.offset.reset", "latest");
        kafkaParamsForRDD.put("enable.auto.commit", false);

        //For KafkaConsumer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("group.id", "RDD_LOADER_FROM_KAFKA");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumer = new KafkaConsumer<>(props);
    }

    private TopicPartition subscribeToTopic(String topicName) {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        TopicPartition topicPartition = new TopicPartition(topicName, 0);
        kafkaConsumer.poll(0);

        return topicPartition;
    }

    private OffsetRange[] getOffsetRanges(TopicPartition topicPartition) {


        kafkaConsumer.seekToEnd(Collections.singletonList(topicPartition));
        long endingOffset = kafkaConsumer.position(topicPartition);
        System.out.println("ending offset is: " + endingOffset);

        kafkaConsumer.seekToBeginning(Collections.singletonList(topicPartition));
        long startingOffset = kafkaConsumer.position(topicPartition);
        System.out.println("Starting position is:" + startingOffset);

        OffsetRange[] offsetRanges = {
                // topic, partition, inclusive starting offset, exclusive ending offset
                OffsetRange.create(topicPartition.topic(), 0, startingOffset, endingOffset)
        };

        return offsetRanges;
    }

    public WeatherRDDLoaderFromKafka(Map<String, CityModel> citiesMap) {
        jsc = SparkContextSingleton.getInstance().getContext();
        cities = citiesMap;

        initKafka();

    }

    public JavaRDD<WeatherMeasurementPojo> loadWeatherMeasurementPojoRDD(String topicName, IMeasurementValidator validator) {

        TopicPartition topicPartition = subscribeToTopic(topicName);
        OffsetRange[] offsetRanges = getOffsetRanges(topicPartition);

        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                jsc,
                kafkaParamsForRDD,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );

        JavaRDD<String> dataSetLines = rdd.map(value -> value.value());
        String csvHeader = dataSetLines.first();

        JavaRDD<String> filteredHeaderRDD = dataSetLines.filter(line -> !line.equals(csvHeader));
        JavaRDD<WeatherMeasurementPojo> resultRDD = filteredHeaderRDD.flatMap(new
                WeatherMeasurementParserFlatMap(csvHeader).setCitiesMap(cities).setValidator(validator)
        );

        return resultRDD;

    }

    public JavaRDD<WeatherDescriptionPojo> loadWeatherDescriptionPojoRDD(String topicName) {

        TopicPartition topicPartition = subscribeToTopic(topicName);
        OffsetRange[] offsetRanges = getOffsetRanges(topicPartition);

        JavaRDD<ConsumerRecord<String, String>> rdd = KafkaUtils.createRDD(
                jsc,
                kafkaParamsForRDD,
                offsetRanges,
                LocationStrategies.PreferConsistent()
        );

        JavaRDD<String> dataSetLines = rdd.map(value -> value.value());
        String csvHeader = dataSetLines.first();

        JavaRDD<String> filteredHeaderRDD = dataSetLines.filter(line -> !line.equals(csvHeader));
        JavaRDD<WeatherDescriptionPojo> resultRDD = filteredHeaderRDD.flatMap(new
                WeatherDescriptionParserFlatMap(csvHeader).setCitiesMap(cities)
        );

        return resultRDD;
    }
}
