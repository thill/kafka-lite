package io.thill.kafkalite.internal;

import java.io.File;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public class KafkaLiteConfig {
  public static final File ZOOKEEPER_DIR = new File(System.getProperty("kafkalite.zk.dir", "zookeeper/"));
  public static final File KAFKA_DIR = new File(System.getProperty("kafkalite.kb.dir", "kafka/"));
  public static final int ZK_PORT = Integer.parseInt(System.getProperty("kafkalite.zk.port", "2181"));
  public static final int KB_PORT = Integer.parseInt(System.getProperty("kafkalite.kb.port", "9092"));
  public static final Map<String, String> KAFKA_PROPERTIES;

  private static final String BROKER_CONFIG_PREFIX = "kafkalite.kb.config";


  static {
    final Map<String, String> kafkaProperties = new LinkedHashMap<>();
    for(Map.Entry<Object, Object> e : System.getProperties().entrySet()) {
      final String key = e.getKey().toString();
      if(key.startsWith(BROKER_CONFIG_PREFIX)) {
         kafkaProperties.put(key.substring(BROKER_CONFIG_PREFIX.length()), e.getValue().toString());
      }
    }

    addIfNotPresent(kafkaProperties, "port", Integer.toString(KB_PORT));
    addIfNotPresent(kafkaProperties, "zookeeper.connect", "localhost:" + ZK_PORT);
    addIfNotPresent(kafkaProperties, "log.dir", KAFKA_DIR.getAbsolutePath());
    addIfNotPresent(kafkaProperties, "broker.id", "1");
    addIfNotPresent(kafkaProperties, "host.name", "localhost");
    addIfNotPresent(kafkaProperties, "log.flush.interval.messages", "1");
    addIfNotPresent(kafkaProperties, "offsets.topic.replication.factor", "1");
    addIfNotPresent(kafkaProperties, "log.retention.ms", "3600000");

    KAFKA_PROPERTIES = Collections.unmodifiableMap(kafkaProperties);
  }

  private static void addIfNotPresent(Map<String, String> config, String key, String value) {
    if(!config.containsKey(key)) {
      config.put(key, value);
    }
  }
}
