/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.client.internal;

import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.pdx.internal.EnumInfo;

/**
 * Retrieve the PDXType, given an integer PDX id, from a server.
 * @since 6.6.2
 */
public class GetPDXEnumByIdOp {
  /**
   * Get a enum from the given pool.
   * @param pool the pool to use to communicate with the server.
   */
  public static EnumInfo execute(ExecutablePool pool,
                             int enumId)
  {
    AbstractOp op = new GetPDXEnumByIdOpImpl(enumId);
    return (EnumInfo) pool.execute(op);
  }
                                                               
  private GetPDXEnumByIdOp() {
    // no instances allowed
  }
  
  private static class GetPDXEnumByIdOpImpl extends AbstractOp {
    /**
     * @throws com.gemstone.gemfire.SerializationException if serialization fails
     */
    public GetPDXEnumByIdOpImpl(int enumId) {
      super(MessageType.GET_PDX_ENUM_BY_ID, 1);
      getMessage().addIntPart(enumId);
    }
    @Override
    protected Object processResponse(Message msg) throws Exception {
      return processObjResponse(msg, "getPDXEnumById");
    }
    @Override
    protected boolean isErrorResponse(int msgType) {
      return false;
    }
    @Override
    protected long startAttempt(ConnectionStats stats) {
      return stats.startGetPDXTypeById(); // reuse PDXType stats instead of adding new enum ones
    }
    @Override
    protected void endSendAttempt(ConnectionStats stats, long start) {
      stats.endGetPDXTypeByIdSend(start, hasFailed());
    }
    @Override
    protected void endAttempt(ConnectionStats stats, long start) {
      stats.endGetPDXTypeById(start, hasTimedOut(), hasFailed());
    }
    @Override
    protected void processSecureBytes(Connection cnx, Message message)
        throws Exception {
    }
    @Override
    protected boolean needsUserId() {
      return false;
    }
    //Don't send the transaction id for this message type.
    @Override
    protected boolean participateInTransaction() {
      return false;
    }
    
    @Override
    protected void sendMessage(Connection cnx) throws Exception {
      getMessage().clearMessageHasSecurePartFlag();
      getMessage().send(false);
    }
  }
}
