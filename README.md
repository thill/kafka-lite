# kafka-lite
A single-node Kafka Server for Unit Testing and Running Locally

## How It Works
A set of idempotent operations built around [Apache Curator](https://github.com/apache/curator)'s [Testing Server](https://github.com/apache/curator/blob/master/curator-test/src/main/java/org/apache/curator/test/TestingServer.java) for [Zookeeper](https://github.com/apache/zookeeper) and [Kafka](https://github.com/apache/kafka/)'s [KafkaStartableServer](https://github.com/apache/kafka/tree/trunk/core/src/main/scala/kafka/server/KafkaServerStartable.scala) class.

## Unit Testing
### Setup
```
@Before
public void start() throws KafkaLiteException {
  KafkaLite.clean(); // all topics should be deleted for the start of each test
  KafkaLite.start(); // ensure kafka is up and running at the start of each test
}
```

## Starting and Stopping
```
KafkaLite.start();
KafkaLite.stop();
KafkaLite.start();
KafkaLite.stop();
```

## Topic Management  
```
KafkaLite.createTopic("my_topic", numPartitions);
KafkaLite.createTopic("my_topic", numPartitions, retentionMillis);
```

## Blocking
All KafkaLite methods block until the operation completes.
```
KafkaLite.start();
// Zookeeper and Kafka are immediately up and reachable
KafkaLite.createTopic("my_topic", 1);
// Topic is immediately available for reading/writing
```

## Idempotence
Repeating calls to the same method should result in the same state without throwing any exceptions.
```
KafkaLite.start();
KafkaLite.start();
KafkaLite.clean();
KafkaLite.clean();
KafkaLite.stop();
KafkaLite.stop();
```

## Running Locally 
```
$ mvn exec:java -Dexec.mainClass="io.thill.kafkalite.KafkaLite" -Dexec.classpathScope="test" -Dexec.args="true"
```

## Cleanup on shutdown
```
KafkaLite.cleanOnShutdown();
```