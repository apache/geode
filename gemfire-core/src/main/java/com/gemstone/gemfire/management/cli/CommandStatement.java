/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.cli;

import java.util.Map;

/**
 * Represents GemFire Command Line Interface (CLI) command strings. A
 * <code>CommandStatement</code> instance can be used multiple times to process
 * the same command string repeatedly.
 * 
 * @author Kirk Lund
 * @author Abhishek Chaudhari
 * 
 * @since 7.0
 */
public interface CommandStatement {
  
  /**
   * Returns the user specified command string.
   */
  public String getCommandString();
  
  /**
   * Returns the CLI environment variables.
   */
  public Map<String, String> getEnv();
  
  /**
   * Processes this command statement with the user specified command string
   * and environment
   * 
   * @return The {@link Result} of the execution of this command statement.
   */
  public Result process();
  
  
  /**
   * Returns whether the command statement is well formed.
   * 
   * @return True if the command statement is well formed, false otherwise.
   */
  public boolean validate();
}
