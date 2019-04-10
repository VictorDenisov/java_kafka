package io.denisov;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Options options = new Options();

        Option proc = new Option("p", "proc", true, "Processor type: consumer, producer");
        proc.setRequired(true);
        options.addOption(proc);

        Option brokers = new Option("b", "brokers", true, "List of brokers");
        brokers.setRequired(true);
        options.addOption(brokers);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
            return;
        }

        String processor = cmd.getOptionValue("proc");

        System.out.println("Processor: " + processor);

        if (processor.equals("consumer")) {
            consumer(cmd.getOptionValue("brokers"));
        } else {
            producer(cmd.getOptionValue("brokers"));
        }
    }

    private static void consumer(String brokers) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("batch.size", 11);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                "KafkaExampleConsumer5");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("isolation.level", "read_uncommitted");

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList("test_topic"));
        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);
            System.out.println("read messages");

/*
            if (consumerRecords.count()==0) {
                break;
            }

 */

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
                for (Header h : record.headers()) {
                    System.out.printf("Consumer header: %s, %s\n", h.key(), new String(h.value()));
                }
            });

            consumer.commitAsync();
        }
        //consumer.close();
        //System.out.println("DONE");
    }

    private static void producer(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("batch.size", 11);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("transactional.id", "my-transactional-id");
        props.put("isolation.level", "read_committed");
        System.out.println("Running producer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        int i = 0;
        ArrayList<Header> headers = new ArrayList<Header>();
        headers.add(new Header() {
            public String key() {
                return "hk";
            }

            public byte[] value() {
                return "hv".getBytes();
            }
        });
        ProducerRecord<String, String> producerRecord;
        producerRecord = new ProducerRecord<String, String>("test_topic", "Key", "Hello World 0!");
        producer.beginTransaction();
        producer.send(producerRecord);
        //producer.abortTransaction();
        producer.commitTransaction();

        System.out.println("Wrote to kafka topic");

        producer.close();
    }
}
