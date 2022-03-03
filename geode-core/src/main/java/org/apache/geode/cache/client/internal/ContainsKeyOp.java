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

import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;

/**
 * Does a region containsKey on a server
 *
 * @since GemFire 5.7
 */
public class ContainsKeyOp {
  /**
   * Does a region entry containsKey on a server using connections from the given pool to
   * communicate with the server.
   *
   * @param pool the pool to use to communicate with the server.
   * @param region the name of the region to do the entry containsKey on
   * @param key the entry key to do the containsKey on
   * @return the result of invoking containsKey on the server
   */
  public static boolean execute(ExecutablePool pool, String region, Object key, MODE mode) {
    AbstractOp op = new ContainsKeyOpImpl(region, key, mode);
    Boolean result = (Boolean) pool.execute(op);
    return result;
  }

  private ContainsKeyOp() {
    // no instances allowed
  }

  private static class ContainsKeyOpImpl extends AbstractOp {

    private final String region;
    private final Object key;
    private final MODE mode;

    /**
     * @throws org.apache.geode.SerializationException if serialization fails
     */
    public ContainsKeyOpImpl(String region, Object key, MODE mode) {
      super(MessageType.CONTAINS_KEY, 3);
      getMessage().addStringPart(region, true);
      getMessage().addStringOrObjPart(key);
      getMessage().addIntPart(mode.ordinal());
      this.region = region;
      this.key = key;
      this.mode = mode;
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      return processObjResponse(msg, "containsKey");
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.CONTAINS_KEY_DATA_ERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startContainsKey();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endContainsKeySend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endContainsKey(start, hasTimedOut(), hasFailed());
    }

    @Override
    public String toString() {
      return "ContainsKeyOp(region=" + region + ";key=" + key + ";mode=" + mode;
    }
  }

  public enum MODE {
    KEY, VALUE_FOR_KEY, VALUE
  }
}
