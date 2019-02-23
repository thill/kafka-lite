package io.thill.kafkalite.exception;

public class KafkaLiteException extends Exception {

  public KafkaLiteException(String message) {
    super(message);
  }

  public KafkaLiteException(String message, Throwable cause) {
    super(message, cause);
  }

}
