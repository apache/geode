package com.gemstone.gemfire.internal.redis;

/**
 * Exception thrown by {@link CommandParser} when a command has illegal syntax
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class RedisCommandParserException extends Exception {

  private static final long serialVersionUID = 4707944288714910949L;

  public RedisCommandParserException() {
    super();
  }

  public RedisCommandParserException(String message) {
    super(message);
  }

  public RedisCommandParserException(Throwable cause) {
    super(cause);
  }

  public RedisCommandParserException(String message, Throwable cause) {
    super(message, cause);
  }

}
