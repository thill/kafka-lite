package io.thill.kafkalite.client;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Queueing Kafka Consumer, useful for tests since it can poll 1 message at a time with a timeout
 */
public class QueuedKafkaConsumer<K, V> implements AutoCloseable {

  private static final Duration KAFKA_POLL_DURATION = Duration.ofMillis(100);

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private final AtomicBoolean keepRunning = new AtomicBoolean(true);
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private final BlockingQueue<ConsumerRecord<K, V>> queue = new LinkedBlockingQueue<>();
  private final TopicPartition topicPartition;
  private final KafkaConsumer<K, V> consumer;

  public QueuedKafkaConsumer(TopicPartition topicPartition, Properties properties) {
    this.topicPartition = topicPartition;
    consumer = new KafkaConsumer<>(properties);
    consumer.assign(Arrays.asList(topicPartition));
    consumer.seekToBeginning(Arrays.asList(topicPartition));
    new Thread(this::run).start();
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
    return poll(1, TimeUnit.SECONDS);
  }

  /**
   * Poll a single record, waiting for up to the given number of milliseconds for it to be available. After the given number of milliseconds, null is returned.
   *
   * @param timeoutMillis The timeout in milliseconds
   * @return The record, of null if it did not exist within the given number of milliseconds
   */
  public ConsumerRecord<K, V> poll(long timeoutMillis) {
    return poll(timeoutMillis, TimeUnit.MILLISECONDS);
  }

  /**
   * Poll a single record, waiting for up to the given time/timeUnit for it to be available. After the timeout, null is returned.
   *
   * @param timeout The timeout value
   * @param unit    The unit of the timeout
   * @return The record, of null if it did not exist within the timeout
   */
  public ConsumerRecord<K, V> poll(long timeout, TimeUnit unit) {
    try {
      return queue.poll(timeout, unit);
    } catch(InterruptedException e) {
      return null;
    }
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
