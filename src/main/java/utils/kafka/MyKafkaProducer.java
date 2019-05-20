package utils.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import utils.configuration.AppConfiguration;

import java.util.List;
import java.util.Properties;

public class MyKafkaProducer {

    private static final String kafkaUri = AppConfiguration.getProperty("kafka.broker.uri");
    KafkaProducer producer;

    public MyKafkaProducer(){

        //For Kafkaproducer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("group.id", "RDD_Result_Producer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        producer = new KafkaProducer(props);

    }

    public void putQueryResultonKafka(String topic, String key, List value) {

        ProducerRecord record = new ProducerRecord(topic,key,value);
        producer.send(record);


    }
}