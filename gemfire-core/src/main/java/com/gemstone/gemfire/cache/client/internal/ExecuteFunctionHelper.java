/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

public class ExecuteFunctionHelper {

  public final static byte BUCKETS_AS_FILTER_MASK = 0x02;
  public final static byte IS_REXECUTE_MASK = 0x01;
  
  static byte createFlags(boolean executeOnBucketSet, byte isReExecute) {
    byte flags = executeOnBucketSet ? 
        (byte)(0x00 | BUCKETS_AS_FILTER_MASK) : 0x00;
    flags = isReExecute == 1? (byte)(flags | IS_REXECUTE_MASK) : flags;      
    return flags;
  }
}
