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

import java.util.Properties;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerConnectivityException;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.serialization.KnownVersion;

public class ProxyCacheCloseOp {

  public static Object executeOn(ServerLocation location, ExecutablePool pool,
      Properties securityProps, boolean keepAlive) {
    AbstractOp op = new ProxyCacheCloseOpImpl(pool, securityProps, keepAlive);
    return pool.executeOn(location, op);
  }

  private ProxyCacheCloseOp() {
    // no instances allowed
  }

  static class ProxyCacheCloseOpImpl extends AbstractOp {

    public ProxyCacheCloseOpImpl(ExecutablePool pool, Properties securityProps, boolean keepAlive) {
      super(MessageType.REMOVE_USER_AUTH, 1);
      getMessage().setMessageHasSecurePartFlag();
      getMessage().addBytesPart(keepAlive ? new byte[] {1} : new byte[] {0});
    }

    @Override
    protected boolean needsUserId() {
      return false;
    }

    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      HeapDataOutputStream hdos = new HeapDataOutputStream(KnownVersion.CURRENT);
      byte[] secureBytes = null;
      hdos.writeLong(cnx.getConnectionID());
      Object userId = UserAttributes.userAttributes.get().getServerToId().get(cnx.getServer());
      if (userId == null) {
        // This will ensure that this op is retried on another server, unless
        // the retryCount is exhausted. Fix for Bug 41501
        throw new ServerConnectivityException("Connection error while authenticating user");
      }
      hdos.writeLong((Long) userId);
      try {
        secureBytes = ((ConnectionImpl) cnx).encryptBytes(hdos.toByteArray());
      } finally {
        hdos.close();
      }
      getMessage().setSecurePart(secureBytes);
      getMessage().send(false);
    }

    @Override
    protected Object processResponse(Message msg) throws Exception {
      Part part = msg.getPart(0);
      final int msgType = msg.getMessageType();
      if (msgType == MessageType.REPLY) {
        return part.getObject();
      } else if (msgType == MessageType.EXCEPTION) {
        String s = "While performing a remote proxy cache close";
        throw new ServerOperationException(s, (Throwable) part.getObject());
        // Get the exception toString part.
        // This was added for c++ thin client and not used in java
      } else if (isErrorResponse(msgType)) {
        throw new ServerOperationException(part.getString());
      } else {
        throw new InternalGemFireError("Unexpected message type " + MessageType.getString(msgType));
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return msgType == MessageType.REQUESTDATAERROR;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGet();
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetSend(start, hasFailed());
    }

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGet(start, hasTimedOut(), hasFailed());
    }
  }

}
