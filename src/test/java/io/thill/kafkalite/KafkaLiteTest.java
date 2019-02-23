package io.thill.kafkalite;

import io.thill.kafkalite.client.KafkaLiteClient;
import io.thill.kafkalite.client.QueuedKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KafkaLiteTest {

  @Before
  public void setup() throws Exception {
    KafkaLite.cleanOnShutdown();
    KafkaLite.reset();
  }

  @Test
  public void testCreateTopic() {
    final String topic = "my_topic";
    final int partition = 3;
    final TopicPartition topicPartition = new TopicPartition(topic, partition);

    KafkaLite.createTopic(topic, partition + 1);

    try(KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class))) {
      producer.send(new ProducerRecord<>(topic, partition, "K1", "V1"));
      producer.send(new ProducerRecord<>(topic, partition, "K2", "V2"));
    }

    try(QueuedKafkaConsumer<String, String> consumer = new QueuedKafkaConsumer<>(topicPartition, KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class))) {
      ConsumerRecord<String, String> r1 = consumer.poll();
      ConsumerRecord<String, String> r2 = consumer.poll();

      Assert.assertNotNull(r1);
      Assert.assertEquals(topic, r1.topic());
      Assert.assertEquals(partition, r1.partition());
      Assert.assertEquals("K1", r1.key());
      Assert.assertEquals("V1", r1.value());

      Assert.assertNotNull(r2);
      Assert.assertEquals(topic, r2.topic());
      Assert.assertEquals(partition, r2.partition());
      Assert.assertEquals("K2", r2.key());
      Assert.assertEquals("V2", r2.value());

      Assert.assertTrue(consumer.isEmpty(100));
    }
  }

  @Test
  public void testDeleteTopic() {
    final String topic = "my_topic";

    try(KafkaLiteClient client = new KafkaLiteClient()) {
      KafkaLite.createTopic(topic, 1);

      Assert.assertTrue(client.kafkaConsumer().listTopics().containsKey(topic));
      Assert.assertTrue(client.kafkaZkClient().topicExists(topic));

      KafkaLite.deleteTopic(topic);

      Assert.assertFalse(client.kafkaConsumer().listTopics().containsKey(topic));
      Assert.assertFalse(client.kafkaZkClient().topicExists(topic));
    }
  }

  @Test
  public void testStartStopStart() throws Exception {
    final String topic = "my_topic";
    final int partition = 3;
    final TopicPartition topicPartition = new TopicPartition(topic, partition);

    // start, create a topic, send and verify a message
    KafkaLite.start();
    KafkaLite.createTopic(topic, partition + 1);
    try(KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class))) {
      producer.send(new ProducerRecord<>(topic, partition, "K100", "V100"));
    }

    try(QueuedKafkaConsumer<String, String> consumer = new QueuedKafkaConsumer<>(topicPartition, KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class))) {
      ConsumerRecord<String, String> r1 = consumer.poll();

      Assert.assertNotNull(r1);
      Assert.assertEquals(topic, r1.topic());
      Assert.assertEquals(partition, r1.partition());
      Assert.assertEquals("K100", r1.key());
      Assert.assertEquals("V100", r1.value());

      Assert.assertTrue(consumer.isEmpty(100));
    }

    // stop
    KafkaLite.stop();

    // start, send a second message, verify messages from before and after restart exist
    KafkaLite.start();

    try(KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaLite.producerProperties(StringSerializer.class, StringSerializer.class))) {
      producer.send(new ProducerRecord<>(topic, partition, "K101", "V101"));
    }

    try(QueuedKafkaConsumer<String, String> consumer = new QueuedKafkaConsumer<>(topicPartition, KafkaLite.consumerProperties(StringDeserializer.class, StringDeserializer.class))) {
      ConsumerRecord<String, String> r1 = consumer.poll();
      ConsumerRecord<String, String> r2 = consumer.poll();

      Assert.assertNotNull(r1);
      Assert.assertEquals(topic, r1.topic());
      Assert.assertEquals(partition, r1.partition());
      Assert.assertEquals("K100", r1.key());
      Assert.assertEquals("V100", r1.value());

      Assert.assertNotNull(r2);
      Assert.assertEquals(topic, r2.topic());
      Assert.assertEquals(partition, r2.partition());
      Assert.assertEquals("K101", r2.key());
      Assert.assertEquals("V101", r2.value());

      Assert.assertTrue(consumer.isEmpty(100));
    }
  }
}
