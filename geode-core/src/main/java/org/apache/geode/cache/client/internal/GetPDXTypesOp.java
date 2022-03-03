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

import java.util.Map;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.pdx.internal.PdxType;

/**
 * Retrieve all known PDX types.
 *
 * @since GemFire 7.0
 */
public class GetPDXTypesOp {

  public static Map<Integer, PdxType> execute(ExecutablePool pool) {
    AbstractOp op = new GetPDXTypesOpImpl();
    return (Map<Integer, PdxType>) pool.execute(op);
  }

  private GetPDXTypesOp() {
    // no instances allowed
  }

  private static class GetPDXTypesOpImpl extends AbstractOp {
    public GetPDXTypesOpImpl() {
      super(MessageType.GET_PDX_TYPES, 1);
      getMessage().addIntPart(0); // must have at least one part
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      Part part = msg.getPart(0);
      int msgType = msg.getMessageType();
      if (msgType == MessageType.RESPONSE) {
        return part.getObject();

      } else {
        if (msgType == MessageType.EXCEPTION) {
          String s = "While performing a remote " + "getPdxTypes";
          throw new ServerOperationException(s, (Throwable) part.getObject());

        } else if (isErrorResponse(msgType)) {
          throw new ServerOperationException(part.getString());

        } else {
          throw new InternalGemFireError(
              "Unexpected message type " + MessageType.getString(msgType));
        }
      }
    }

    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }

    @Override
    protected long startAttempt(ConnectionStats stats) {
      return 0;
    }

    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {}

    @Override
    protected void endAttempt(ConnectionStats stats, long start) {}

    @Override
    protected boolean participateInTransaction() {
      return false;
    }

  }
}
