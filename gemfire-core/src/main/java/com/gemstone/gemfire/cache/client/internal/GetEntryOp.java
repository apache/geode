/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.EntrySnapshot;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;

/**
 * does getEntry on the server
 * @author sbawaska
 */
public class GetEntryOp {

  /**
   * Does a region.getEntry on the server using the given pool
   * @param pool
   * @param region
   * @param key
   * @return an {@link EntrySnapshot} for the given key
   */
  public static Object execute(ExecutablePool pool, LocalRegion region,
      Object key) {
    AbstractOp op = new GetEntryOpImpl(region, key);
    return pool.execute(op);
  }
  
  static class GetEntryOpImpl extends AbstractOp {

    private LocalRegion region;
    private Object key;
    public GetEntryOpImpl(LocalRegion region, Object key) {
      super(MessageType.GET_ENTRY, 2);
      this.region = region;
      this.key = key;
      getMessage().addStringPart(region.getFullPath());
      getMessage().addStringOrObjPart(key);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      EntrySnapshot snap = (EntrySnapshot) processObjResponse(msg, "getEntry");
      if (snap != null) {
        snap.region = region;
      }
      return snap;
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override  
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetEntry();
    }
    @Override  
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetEntrySend(start, hasFailed());
    }
    @Override  
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetEntry(start, hasTimedOut(), hasFailed());
    }
  }
}
