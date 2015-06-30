package com.gemstone.gemfire.internal.redis;

/**
 * This exception is for the case that a client attempts to operate on
 * a data structure of one {@link RedisDataType} with a command that is 
 * of another type
 * 
 * @author Vitaliy Gavrilov
 *
 */
public class RedisDataTypeMismatchException extends RuntimeException {

  private static final long serialVersionUID = -2451663685348513870L;
  
  public RedisDataTypeMismatchException() {
    super();
  }

  public RedisDataTypeMismatchException(String message) {
    super(message);
  }

}
