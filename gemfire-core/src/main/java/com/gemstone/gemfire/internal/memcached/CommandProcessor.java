/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * All memcached commands specified by {@link Command}
 * implement this interface to process a command from
 * the client.
 * 
 * @author Swapnil Bawaskar
 *
 */
public interface CommandProcessor {

  /**
   * 
   * @param reader
   * @param protocol
   * @param cache 
   */
  public ByteBuffer processCommand(RequestReader reader, Protocol protocol, Cache cache);
  
}
