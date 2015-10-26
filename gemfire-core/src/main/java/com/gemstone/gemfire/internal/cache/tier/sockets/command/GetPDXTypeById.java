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
/**
 * 
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.pdx.internal.PdxType;
import com.gemstone.gemfire.pdx.internal.TypeRegistry;


public class GetPDXTypeById extends BaseCommand {

  private final static GetPDXTypeById singleton = new GetPDXTypeById();

  public static Command getCommand() {
    return singleton;
  }

  private GetPDXTypeById() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException {
    servConn.setAsTrue(REQUIRES_RESPONSE);
    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received get pdx type by id request ({} parts) from {}", servConn.getName(), msg.getNumberOfParts(), servConn.getSocketString());
    }
    int pdxId = msg.getPart(0).getInt();
    
    PdxType type;
    try {
      GemFireCacheImpl cache = (GemFireCacheImpl) servConn.getCache();
      TypeRegistry registry = cache.getPdxRegistry();
      type = registry.getType(pdxId);
    } catch (Exception e) {
      writeException(msg, e, false, servConn);
      servConn.setAsTrue(RESPONDED);
      return;
    }

    Message responseMsg = servConn.getResponseMessage();
    responseMsg.setMessageType(MessageType.RESPONSE);
    responseMsg.setNumberOfParts(1);
    responseMsg.setTransactionId(msg.getTransactionId());
    responseMsg.addObjPart(type);
    responseMsg.send(servConn);
    servConn.setAsTrue(RESPONDED);
  }
}
