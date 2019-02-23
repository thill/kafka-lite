package io.thill.kafkalite.client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queueing Kafka Consumer, useful for tests since it can poll 1 message at a time with a timeout
 */
public class QueuedKafkaConsumer<K, V> implements AutoCloseable {

  private static final Duration KAFKA_POLL_DURATION = Duration.ofMillis(100);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private final Queue<ConsumerRecord<K, V>> queue = new LinkedList<>();
  private final TopicPartition topicPartition;
  private final KafkaConsumer<K, V> consumer;

  public QueuedKafkaConsumer(TopicPartition topicPartition, Properties properties) {
    this.topicPartition = topicPartition;
    consumer = new KafkaConsumer<>(properties);
    consumer.assign(Arrays.asList(topicPartition));
    consumer.seekToBeginning(Arrays.asList(topicPartition));
  }

  /**
   * Seek to beginning of topic partition
   */
  public void seekToBeginning() {
    consumer.seekToEnd(Arrays.asList(topicPartition));
  }

  /**
   * Seek to end of topic partition
   */
  public void seekToEnd() {
    consumer.seekToEnd(Arrays.asList(topicPartition));
  }

  private void run() {
    try {
      while(keepRunning.get()) {
        for(ConsumerRecord<K, V> record : consumer.poll(KAFKA_POLL_DURATION)) {
          queue.add(record);
        }
      }
    } finally {
      closedLatch.countDown();
    }
  }

  /**
   * Poll a single record, waiting for up to one second for it to be available. After 1 second, null is returned.
   *
   * @return The record, or null if it did not exist within 1 second
   */
  public ConsumerRecord<K, V> poll() {
    return poll(Duration.ofSeconds(1));
  }

  /**
   * Poll a single record, waiting for up to the given timeout for it to be available. After the timeout, null is returned.
   *
   * @param timeout The timeout duration
   * @return The record, of null if it did not exist within the timeout
   */
  public ConsumerRecord<K, V> poll(Duration timeout) {
    if(queue.size() > 0) {
      for(ConsumerRecord<K, V> r : consumer.poll(timeout)) {
        queue.add(r);
      }
    }
    return queue.poll();
  }

  public boolean isEmpty() {
    return isEmpty(0);
  }

  public boolean isEmpty(long waitMillis) {
    try {
      Thread.sleep(waitMillis);
    } catch(InterruptedException e) {
      // move on
    }
    return queue.isEmpty();
  }

  @Override
  public void close() {
    keepRunning.set(false);
    try {
      closedLatch.await();
    } catch(InterruptedException e) {
      logger.warn("Interrupted", e);
    }
  }
}
