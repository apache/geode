/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.memcached.commands;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.internal.memcached.Command;
import com.gemstone.gemfire.internal.memcached.RequestReader;
import com.gemstone.gemfire.memcached.GemFireMemcachedServer.Protocol;

/**
 * Does not send a response on a cache miss.
 * 
 * @author Swapnil Bawaskar
 */
public class GetQCommand extends GetCommand {

  @Override
  protected boolean isQuiet() {
    return true;
  }
}
