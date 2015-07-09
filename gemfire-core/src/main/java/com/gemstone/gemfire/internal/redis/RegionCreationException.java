package com.gemstone.gemfire.internal.redis;

public class RegionCreationException extends RuntimeException {

  public RegionCreationException() {}
  
  public RegionCreationException(String err) {
    super(err);
  }
  
  public RegionCreationException(String err, Throwable cause) {
    super(err, cause);
  }

  /**
   * 
   */
  private static final long serialVersionUID = 8416820139078312997L;

}
