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

public class GetFunctionAttributeOp {

  public static Object execute(ExecutablePool pool, String functionId) {
    AbstractOp op = new GetFunctionAttributeOpImpl(functionId);
    return pool.execute(op);
  }

  private GetFunctionAttributeOp() {
    // no instances allowed
  }

  static class GetFunctionAttributeOpImpl extends AbstractOp {

    private String functionId = null;

    public GetFunctionAttributeOpImpl(String functionId) {
      super(MessageType.GET_FUNCTION_ATTRIBUTES, 1);
      this.functionId = functionId;
      getMessage().addStringPart(this.functionId);
    }

    @Override
    protected Object processResponse(final @NotNull Message msg) throws Exception {
      return processObjResponse(msg, "getFunctionAttribute");
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
