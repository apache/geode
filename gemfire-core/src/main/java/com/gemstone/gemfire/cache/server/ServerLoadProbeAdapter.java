/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.server;


/**
 * Utility class that implements all methods in {@link ServerLoadProbe} with
 * empty implementations for open and close. Applications can subclass this
 * class and only override the methods for the events of interest.
 * 
 * @since 5.7
 * @author dsmith
 * 
 */
public abstract class ServerLoadProbeAdapter implements ServerLoadProbe {

  /**
   * Does nothing.
   */
  public void close() {
  }
  
  /**
   * Does nothing.
   */
  public void open() {
  }

}
