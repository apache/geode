package com.gemstone.gemfire.internal.process;

/**
 * Exception thrown during server startup when  it requests the locators for shared configuration and does not receive it.
 * 
 * @author bansods
 * @since 8.0
 */
public final class ClusterConfigurationNotAvailableException extends RuntimeException {
  private static final long serialVersionUID = -3448160213553925462L;
  
  public ClusterConfigurationNotAvailableException(String message) {
    super(message);
  }
}
