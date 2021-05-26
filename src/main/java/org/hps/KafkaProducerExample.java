package org.hps;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class KafkaProducerExample {
    private static final Logger log = LogManager.getLogger(KafkaProducerExample.class);

    private static Instant start = null;

    public static void main(String[] args) throws InterruptedException, IOException, URISyntaxException {
        Workload wrld = new Workload();

        KafkaProducerConfig config = KafkaProducerConfig.fromEnv();
        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());
        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        KafkaProducer producer = new KafkaProducer(props);
        log.info("Sending {} messages ...", config.getMessageCount());
        boolean blockProducer = System.getenv("BLOCKING_PRODUCER") != null;

        AtomicLong numSent = new AtomicLong(0);

         // over all the workload
        for (int i = 0; i < wrld.getDatax().size(); i++) {

            //   loop over each sample
            for (long j = 0; j < Math.ceil(wrld.getDatay().get(i)); j++) {

                log.info("Sending messages \"" + config.getMessage() + " - {}\"{}", i);
                Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord(config.getTopic(),
                        null, null,
                        UUID.randomUUID().toString(), "\"" + config.getMessage() + " - " + i));
                if(blockProducer) {
                    try {
                        recordMetadataFuture.get();
                        // Increment number of sent messages only if ack is received by producer
                        numSent.incrementAndGet();
                    } catch (ExecutionException e) {
                        log.warn("Message {} wasn't sent properly!", i, e.getCause());
                    }
                } else {
                    // Increment number of sent messages for non blocking producer
                    numSent.incrementAndGet();
                }


            }


            log.info("{} messages sent ...", Math.ceil(wrld.getDatay().get(i)));

            log.info("sleeping for 15 seconds");



            Thread.sleep(15000);




        }

        producer.close();

        log.info("all {} messages sent ...", Math.ceil(wrld.getDatay().size()));



    }
        /*KafkaProducerConfig config = KafkaProducerConfig.fromEnv();

        log.info(KafkaProducerConfig.class.getName() + ": {}", config.toString());

        Properties props = KafkaProducerConfig.createProperties(config);
        int delay = config.getDelay();
        KafkaProducer producer = new KafkaProducer(props);
        log.info("Sending {} messages ...", config.getMessageCount());
        boolean blockProducer = System.getenv("BLOCKING_PRODUCER") != null;

        AtomicLong numSent = new AtomicLong(0);

        start = Instant.now();
        for (long i = 0; i < config.getMessageCount(); i++) {

            log.info("Sending messages \"" + config.getMessage() + " - {}\"{}", i);
            Future<RecordMetadata> recordMetadataFuture = producer.send(new ProducerRecord(config.getTopic(),
                    null, null,
                    UUID.randomUUID().toString(), "\"" + config.getMessage() + " - " + i));
            if(blockProducer) {
                try {
                    recordMetadataFuture.get();
                    // Increment number of sent messages only if ack is received by producer
                    numSent.incrementAndGet();
                } catch (ExecutionException e) {
                    log.warn("Message {} wasn't sent properly!", i, e.getCause());
                }
            } else {
                // Increment number of sent messages for non blocking producer
                numSent.incrementAndGet();
            }

            Thread.sleep(delay);
            Instant now = Instant.now();

            long elapsedTime = Duration.between(start, now).toMinutes();
            if(elapsedTime <= 2) {
                // 2 messages per second

                delay = 500;
            } else if( elapsedTime <= 5) {
                // 3 messages per second
                delay = 333;
            } else if(elapsedTime <= 7) {
                // 4 messages per second
                delay = 250;
            } else if(elapsedTime <= 8) {
                // 10 messages per second.
                delay = 100;
            } else if(elapsedTime <= 9) {
                delay = 2000;
                start = Instant.now();
            }

        }

        System.exit(numSent.get() == config.getMessageCount() ? 0 : 1);
    }*/




}
