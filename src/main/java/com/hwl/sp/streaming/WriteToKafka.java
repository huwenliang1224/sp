package com.hwl.sp.streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class WriteToKafka {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "47.104.148.179:9092,47.104.148.179:9093,47.104.148.179:9094");
        props.put("acks", "0");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        try {
            InputStream is = WriteToKafka.class.getResourceAsStream("/LICENSE.txt");
            InputStreamReader isr = new InputStreamReader(is, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                producer.send(new ProducerRecord("test", line));
            }
            is.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.close();
    }
}
