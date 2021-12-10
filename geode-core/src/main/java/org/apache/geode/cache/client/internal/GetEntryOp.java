/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.cache.client.internal;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.internal.cache.EntrySnapshot;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * does getEntry on the server
 */
public class GetEntryOp {

  /**
   * Does a region.getEntry on the server using the given pool
   *
   * @return an {@link EntrySnapshot} for the given key
   */
  public static Object execute(ExecutablePool pool, LocalRegion region, Object key) {
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
      getMessage().addStringPart(region.getFullPath(), true);
      getMessage().addStringOrObjPart(key);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      EntrySnapshot snap = (EntrySnapshot) processObjResponse(msg, "getEntry");
      if (snap != null) {
        snap.setRegion(region);
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
