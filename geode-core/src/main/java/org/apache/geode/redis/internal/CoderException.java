package org.apache.geode.redis.internal;

public class CoderException extends Exception {
  private static final long serialVersionUID = 4707944288714910949L;

  public CoderException() {
    super();
  }

  public CoderException(String message) {
    super(message);
  }

  public CoderException(Throwable cause) {
    super(cause);
  }

  public CoderException(String message, Throwable cause) {
    super(message, cause);
  }
}
