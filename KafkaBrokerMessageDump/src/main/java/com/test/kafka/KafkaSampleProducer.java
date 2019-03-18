package com.test.kafka;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.common.util.RandomUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A sample producer which will create sample data to kafka topic
 */
public class KafkaSampleProducer 
{

    private static final Logger logger = LoggerFactory.getLogger(KafkaSampleProducer.class);
    @SuppressWarnings("static-access")
    private static final Option OPTION_TOPIC = OptionBuilder.withArgName("topic").hasArg().isRequired(true).withDescription("Kafka topic").create("topic");
    private static final Option OPTION_BROKER = OptionBuilder.withArgName("broker").hasArg().isRequired(true).withDescription("Kafka broker").create("broker");
    private static final Option OPTION_INTERVAL = OptionBuilder.withArgName("interval").hasArg().isRequired(false).withDescription("Simulated message interval in mili-seconds, default 1000").create("interval");

    protected static final String OTHER = "Other";
    private static final ObjectMapper mapper = new ObjectMapper();

    public void kafkaDataCreator(int count,int userInterval, String userTopic, String userBroker) throws Exception 
    {
        //OptionsHelper optionsHelper = new OptionsHelper();
        //Options options = new Options();
        //options.addOption(OPTION_TOPIC);
        //options.addOption(OPTION_BROKER);
        //options.addOption(OPTION_INTERVAL);
        //optionsHelper.parseOptions(options, args);

        //logger.info("options: '{}'", optionsHelper.getOptionsAsString());

        String topic = "Demo";//optionsHelper.getOptionValue(OPTION_TOPIC);
        if(userTopic!=null && userTopic.trim().length()>0)
        {
        	topic=userTopic;
        }
        
        String broker = "10.128.169.162:9092";//optionsHelper.getOptionValue(OPTION_BROKER);
        if(userBroker!=null && userBroker.trim().length()>0)
        {
        	broker=userBroker;
        }
        
        long interval = 10;
        if (userInterval != 0) 
        {
            interval = userInterval;
        }
        System.out.println ("Sending data to Broker: "+ broker+" ,topic: "+topic+ " ,At interval : " +interval+" , Count of message :" +count);

        List<String> ID = new ArrayList<>();
        ID.add("id_14");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        ID.add("id_15");
        
        List<String> INCOMINGDATARATE = new ArrayList<>();
        INCOMINGDATARATE.add("1234");
        INCOMINGDATARATE.add("123");
        INCOMINGDATARATE.add("12");
        INCOMINGDATARATE.add("1");
        INCOMINGDATARATE.add("999");
        INCOMINGDATARATE.add("99");
        INCOMINGDATARATE.add("100");
        INCOMINGDATARATE.add("167");
        INCOMINGDATARATE.add("9");
        INCOMINGDATARATE.add("300");

        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        long startTime = System.currentTimeMillis();
        try (Producer<String, String> producer = new KafkaProducer<>(props)) 
        {
            boolean alive = true;
            Random rnd = new Random();
            Map<String, Object> record = new HashMap<>();
            int x=0;
            //while (alive && count>x) 
            while (count>x) 	
            {
                //add normal record
                record.put("ID", ID.get(rnd.nextInt(ID.size())));
                record.put("ENTRYDATE", "20180101");
                record.put("INCOMINGDATARATE", INCOMINGDATARATE.get(rnd.nextInt(INCOMINGDATARATE.size())));
                record.put("event_time", "1548137755356");

                //send message
                ProducerRecord<String, String> data = new ProducerRecord<>(topic, System.currentTimeMillis() + "", mapper.writeValueAsString(record));
                logger.info("Sending 1 message: {}", JsonUtil.writeValueAsString(record));
                producer.send(data);
                Thread.sleep(interval);
                /**
                if(System.currentTimeMillis() - startTime <= 7 * 24 * 3600 * 1000)
                {
                    alive = false;
                }
                **/
                x++;
            }
        }
    }
}

