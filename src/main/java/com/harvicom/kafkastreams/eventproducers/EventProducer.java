package com.harvicom.kafkastreams.processor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.io.ClassPathResource;

import java.util.Properties;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Date;
import java.text.SimpleDateFormat;

import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        String topicName="";

        try (InputStream input = new ClassPathResource("EventProducer.properties").getInputStream()) {

            Properties prop = new Properties();
            // load a properties file
            prop.load(input);
            props.put(ProducerConfig.CLIENT_ID_CONFIG, prop.getProperty("EventProducer.producerApplicationID"));
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("EventProducer.bootstrapServers"));
            topicName=prop.getProperty("topicName");
        } catch (IOException ex) {
            logger.error(ex.getMessage());
        }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);

        BufferedReader reader;
        ObjectMapper omapper = new ObjectMapper();
        ObjectReader oreader = omapper.reader();
        String outputLine = "";

        String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date());

		try {
			//reader = new BufferedReader(new FileReader("SampleTransactions.txt"));
            reader = new BufferedReader(new InputStreamReader(new ClassPathResource("SampleTransactions.txt").getInputStream()));
			String line = reader.readLine();

            int i=1;
            logger.info("Start sending messages...");
			while (line != null) {

                JsonNode node = oreader.readTree(line);
                ObjectNode objectNode = (ObjectNode) node;
                objectNode.put("TRANASACTION_TIME",timeStamp);
                outputLine=node.toString();
                System.out.println(outputLine);
                producer.send(new ProducerRecord<>(topicName, i,outputLine));
				// read next line
				line = reader.readLine();
                i++;
			}

			reader.close();
		
		} catch (JsonProcessingException jpe) {
            logger.error(jpe.getMessage());
        } catch (IOException ioe) {
			logger.error(ioe.getMessage());
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();
    }
}
