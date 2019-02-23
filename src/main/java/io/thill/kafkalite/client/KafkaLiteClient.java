package io.thill.kafkalite.client;

import io.thill.kafkalite.KafkaLite;
import kafka.zk.AdminZkClient;
import kafka.zk.KafkaZkClient;
import kafka.zookeeper.ZooKeeperClient;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import static io.thill.kafkalite.internal.KafkaLiteConfig.*;

/**
 * Superset of relevant Zookeeper and Kafka clients for managing a KafkaLite instance
 */
public class KafkaLiteClient implements AutoCloseable {
  private final Time time = new SystemTime();
  private final ZooKeeperClient zkClient;
  private final KafkaZkClient kafkaZkClient;
  private final AdminZkClient adminZkClient;
  private final KafkaConsumer<byte[], byte[]> kafkaConsumer;

  public KafkaLiteClient() {
    zkClient = new ZooKeeperClient("localhost:" + ZK_PORT, 1000, 1000, 1, time, "DEFAULT", "DEFAULT");
    kafkaZkClient = new KafkaZkClient(zkClient, false, time);
    adminZkClient = new AdminZkClient(kafkaZkClient);
    kafkaConsumer = new KafkaConsumer<>(KafkaLite.consumerProperties(ByteArrayDeserializer.class, ByteArrayDeserializer.class));
  }

  public ZooKeeperClient zkClient() {
    return zkClient;
  }

  public KafkaZkClient kafkaZkClient() {
    return kafkaZkClient;
  }

  public AdminZkClient adminZkClient() {
    return adminZkClient;
  }

  public KafkaConsumer<byte[], byte[]> kafkaConsumer() {
    return kafkaConsumer;
  }

  @Override
  public void close() {
    kafkaConsumer.close();
    kafkaZkClient.close();
    zkClient.close();
  }
}
