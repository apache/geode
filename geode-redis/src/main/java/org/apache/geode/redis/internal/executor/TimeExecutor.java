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
package org.apache.geode.redis.internal.executor;

import io.netty.buffer.ByteBuf;

import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;

public class TimeExecutor extends AbstractExecutor {

  @Override
  public void executeCommand(Command command, ExecutionHandlerContext context) {
    long timeStamp = System.currentTimeMillis();
    long seconds = timeStamp / 1000;
    long microSeconds = (timeStamp - (seconds * 1000)) * 1000;
    byte[] secAr = Coder.longToBytes(seconds);
    byte[] micAr = Coder.longToBytes(microSeconds);

    ByteBuf response = context.getByteBufAllocator().buffer(50);
    response.writeByte(Coder.ARRAY_ID);
    response.writeByte(50); // #2
    response.writeBytes(Coder.CRLFar);
    response.writeByte(Coder.BULK_STRING_ID);
    response.writeBytes(Coder.intToBytes(secAr.length));
    response.writeBytes(Coder.CRLFar);
    response.writeBytes(secAr);
    response.writeBytes(Coder.CRLFar);
    response.writeByte(Coder.BULK_STRING_ID);
    response.writeBytes(Coder.intToBytes(micAr.length));
    response.writeBytes(Coder.CRLFar);
    response.writeBytes(micAr);
    response.writeBytes(Coder.CRLFar);
    command.setResponse(response);
  }
}
