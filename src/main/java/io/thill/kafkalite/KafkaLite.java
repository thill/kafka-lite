package io.thill.kafkalite;

import io.thill.kafkalite.client.KafkaLiteClient;
import io.thill.kafkalite.exception.KafkaLiteException;
import kafka.admin.RackAwareMode.Disabled$;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.curator.test.InstanceSpec;
import org.apache.curator.test.TestingServer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Signal;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static io.thill.kafkalite.internal.KafkaLiteConfig.*;

/**
 * Manage the lifecycle and topics
 *
 * @author Eric Thill
 */
public class KafkaLite {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaLite.class);
  private static final long IDLE_MILLIS = 100;

  private static boolean started = false;
  private static boolean cleanOnShutdown = false;
  private static TestingServer zookeeper;
  private static KafkaServerStartable kafka;

  /**
   * Main method to start a KafkaLite instance. The KafkaLite instance will be stopped upon receiving a SIGINT signal.
   *
   * @param args
   */
  public static void main(String... args) throws KafkaLiteException {
    boolean clean = args.length > 0 && "true".equals(args[0]);
    LOGGER.info("Clean: {}", clean);
    if(clean) {
      KafkaLite.clean();
      KafkaLite.cleanOnShutdown();
    }
    KafkaLite.start();
    Signal.handle(new Signal("INT"), (signal) -> KafkaLite.stop());
  }

  /**
   * Clean and start
   */
  public static synchronized void reset() throws KafkaLiteException {
    LOGGER.debug("reset()");
    clean();
    start();
  }

  /**
   * If not already started, start Zookeeper and Kafka now. The method will block until fully started.
   */
  public static synchronized void start() throws KafkaLiteException {
    LOGGER.debug("start()");
    if(!started) {
      if(!KAFKA_DIR.isDirectory()) {
        LOGGER.info("Creating {}", KAFKA_DIR.getAbsolutePath());
        KAFKA_DIR.mkdirs();
      }
      LOGGER.info("Starting Zookeeper on port {}", ZK_PORT);
      try {
        zookeeper = new TestingServer(new InstanceSpec(ZOOKEEPER_DIR, ZK_PORT, -1, -1, false, -1), true);
        zookeeper.start();
      } catch(Throwable t) {
        zookeeper = close(zookeeper);
        throw new KafkaLiteException("Could not start Zookeeper", t);
      }
      LOGGER.info("Starting Kafka on port {}", KB_PORT);
      try {
        kafka = new KafkaServerStartable(new KafkaConfig(KAFKA_PROPERTIES));
        kafka.startup();
      } catch(Throwable t) {
        zookeeper = close(zookeeper);
        kafka = close(kafka);
        throw new KafkaLiteException("Could not start Kafka", t);
      }
      try {
        Thread.sleep(3000);
      } catch(InterruptedException e) {
        e.printStackTrace();
      }
      try(KafkaLiteClient client = new KafkaLiteClient()) {
        LOGGER.info("Existing topics: {}", client.kafkaConsumer().listTopics().keySet());
      }
      started = true;
    }
  }

  /**
   * If started, stop Kafka and Zookeeper now. This method will block until fully stopped.
   */
  public static synchronized void stop() {
    LOGGER.debug("stop()");
    if(started) {
      LOGGER.info("Stopping Kafka");
      kafka = close(kafka);
      LOGGER.info("Stopping Zookeeper");
      zookeeper = close(zookeeper);
      started = false;
    }
  }

  /**
   * Delete all existing Kafka topics. If started, delete topic commands will be send for all existing topics.  If stopped, delete the Kafka directory will be
   * deleted. This method blocks until the topics have been fully deleted.
   */
  public static synchronized void clean() {
    LOGGER.debug("clean()");
    if(started) {
      LOGGER.info("Deleting Topics");
      try(KafkaLiteClient client = new KafkaLiteClient()) {
        for(String topic : client.kafkaConsumer().listTopics().keySet()) {
          deleteTopic(topic, client);
        }
      }
    } else {
      LOGGER.info("Deleting {}", KAFKA_DIR.getAbsolutePath());
      deleteRecursively(KAFKA_DIR);
      LOGGER.info("Deleting {}", ZOOKEEPER_DIR.getAbsolutePath());
      deleteRecursively(ZOOKEEPER_DIR);
      while(KAFKA_DIR.exists() || ZOOKEEPER_DIR.exists()) {
        idle();
      }
    }
  }

  /**
   * If started, create a topic on the running Kafka instance with a 1 hour retention.
   *
   * @param topic
   * @param partitions
   * @return true if Kafka was running, false otherwise
   */
  public static synchronized boolean createTopic(String topic, int partitions) {
    return createTopic(topic, partitions, TimeUnit.HOURS.toMillis(1));
  }

  /**
   * If started, create a topic on the running Kafka instance.
   *
   * @param topic
   * @param partitions
   * @param retentionMillis
   * @return true if Kafka was running, false otherwise
   */
  public static synchronized boolean createTopic(String topic, int partitions, long retentionMillis) {
    LOGGER.debug("createTopic({}, {}, {})", topic, partitions, retentionMillis);
    if(started) {
      try(KafkaLiteClient client = new KafkaLiteClient()) {
        List<PartitionInfo> existing = client.kafkaConsumer().listTopics().get(topic);
        if(existing != null) {
          if(existing.size() == partitions) {
            LOGGER.debug("Topic {} already exists", topic);
            return true;
          } else {
            LOGGER.info("Topic {} already exists with different number of partitions. Deleting and recreating...", topic);
            deleteTopic(topic);
          }
        }
        Properties props = new Properties();
        props.setProperty("retention.ms", Long.toString(retentionMillis));
        props.setProperty("preallocate", "false");
        client.adminZkClient().createTopic(topic, partitions, 1, props, new Disabled$());
        while(!client.kafkaZkClient().topicExists(topic)) {
          idle();
        }
        while(!client.kafkaConsumer().listTopics().containsKey(topic)) {
          idle();
        }
      }
      return true;
    } else {
      return false;
    }
  }

  /**
   * If started, delete a topic on the running Kafka instance.
   *
   * @param topic
   * @return true if Kafka was running, false otherwise
   */
  public static synchronized boolean deleteTopic(String topic) {
    LOGGER.debug("deleteTopic({})", topic);
    if(started) {
      try(KafkaLiteClient client = new KafkaLiteClient()) {
        deleteTopic(topic, client);
      }
      return true;
    } else {
      return false;
    }
  }

  private static synchronized void deleteTopic(String topic, KafkaLiteClient client) {
    if(client.kafkaZkClient().topicExists(topic)) {
      LOGGER.info("Deleting Topic {}", topic);
      client.adminZkClient().deleteTopic(topic);
      while(client.kafkaZkClient().topicExists(topic)) {
        idle();
      }
      while(client.kafkaConsumer().listTopics().containsKey(topic)) {
        idle();
      }
    }
  }

  /**
   * Clean kafka and zookeeper directories on JVM shutdown
   */
  public static synchronized void cleanOnShutdown() {
    if(!cleanOnShutdown) {
      cleanOnShutdown = true;
      // stop and remove directories on shutdown
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        stop();
        clean();
      }));
    }
  }

  private static void deleteRecursively(File f) {
    if(f.isDirectory()) {
      for(File child : f.listFiles()) {
        deleteRecursively(child);
      }
    }
    f.delete();
  }

  private static void idle() {
    try {
      Thread.sleep(IDLE_MILLIS);
    } catch(InterruptedException e) {
      LOGGER.warn("Interrupted", e);
      // InterruptedException is not meant to be part of the API, so throw a Runtime Exception to fail the test instead.
      throw new RuntimeException("Interrupted", e);
    }
  }

  private static TestingServer close(TestingServer zookeeper) {
    if(zookeeper != null) {
      try {
        zookeeper.close();
      } catch(Throwable t) {
        LOGGER.error("Could not close " + zookeeper.getClass().getName(), t);
      }
    }
    return null;
  }

  private static KafkaServerStartable close(KafkaServerStartable kafka) {
    if(kafka != null) {
      try {
        kafka.shutdown();
        kafka.awaitShutdown();
      } catch(Throwable t) {
        LOGGER.error("Could not shutdown " + kafka.getClass().getName(), t);
      }
    }
    return null;
  }

  /**
   * Create a consumer properties using the given deserializers
   *
   * @param keyDerserializerType  The key deserializer type
   * @param valueDeserializerType The value deserializer type
   * @return The consumer properties
   */
  public static Properties consumerProperties(Class<?> keyDerserializerType, Class<?> valueDeserializerType) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:" + KB_PORT);
    props.setProperty("key.deserializer", keyDerserializerType.getName());
    props.setProperty("value.deserializer", valueDeserializerType.getName());
    return props;
  }

  /**
   * Create a producer properties using the given serializers
   *
   * @param keySerializerType   The key serializer type
   * @param valueSerializerType The value serializer type
   * @return The producer properties
   */
  public static Properties producerProperties(Class<?> keySerializerType, Class<?> valueSerializerType) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", "localhost:" + KB_PORT);
    props.setProperty("key.serializer", keySerializerType.getName());
    props.setProperty("value.serializer", valueSerializerType.getName());
    return props;
  }
}
