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
    private static long iteration =0;
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
            log.info("sending a batch of authorizations of size:{}",
                    Math.ceil(wrld.getDatay().get(i)));
            //   loop over each sample
            for (long j = 0; j < Math.ceil(wrld.getDatay().get(i)); j++) {
               /* log.info("Sending messages \"" + config.getMessage() + " - {}\"{}", i);*/
                Future<RecordMetadata> recordMetadataFuture =
                        producer.send(new ProducerRecord(config.getTopic(),
                        null, null,
                        UUID.randomUUID().toString() /*null*/, "\"" + config.getMessage() + " - " + i));
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
            log.info("iteration{}", iteration);
            log.info("{} messages sent ...", Math.ceil(wrld.getDatay().get(i)));
            iteration++;
            log.info("sleeping for {} seconds", delay);
            Thread.sleep(delay);
        }
       // producer.metrics().forEach();
        producer.close();
        log.info("all {} messages sent ...", Math.ceil(wrld.getDatay().size()));
    }
}
