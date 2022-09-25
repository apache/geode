
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

package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.Logger;

import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.cache.versions.ConcurrentCacheModificationException;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.StaticSerialization;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class ExtendedReplyMessage extends ReplyMessage {
  private static final Logger logger = LogService.getLogger();

  protected Map<String, Integer> gatewayMap = null;

  @Override
  public int getDSFID() {
    return EXTENDED_REPLYMESSAGE;
  }

  public Map<String, Integer> getGatewayMap() {
    return gatewayMap;
  }

  public static void send(InternalDistributedMember recipient, int processorId,
      ReplyException exception, ReplySender dm, boolean ignored, boolean closed,
      boolean sendViaJGroups, boolean internal, Map<String, Integer> gayewayMap) {
    Assert.assertTrue(recipient != null, "Sending a ExtendedReplyMessage to ALL");
    ExtendedReplyMessage m = new ExtendedReplyMessage();

    m.processorId = processorId;
    m.ignored = ignored;
    if (exception != null) {
      m.returnValue = exception;
      m.returnValueIsException = true;
    }
    m.closed = closed;
    m.sendViaJGroups = sendViaJGroups;
    m.gatewayMap = gayewayMap;
    if (logger.isDebugEnabled()) {
      if (exception != null && ignored) {
        if (exception.getCause() instanceof InvalidDeltaException) {
          logger.debug("Replying with invalid-delta: {}", exception.getCause().getMessage());
        } else {
          logger.debug("Replying with ignored=true and exception: {}", m, exception);
        }
      } else if (exception != null) {
        if (exception.getCause() != null
            && (exception.getCause() instanceof EntryNotFoundException)) {
          logger.debug("Replying with entry-not-found: {}", exception.getCause().getMessage());
        } else if (exception.getCause() != null
            && (exception.getCause() instanceof ConcurrentCacheModificationException)) {
          logger.debug("Replying with concurrent-modification-exception");
        } else {
          logger.debug("Replying with exception: {}", m, exception);
        }
      } else if (ignored) {
        logger.debug("Replying with ignored=true: {}", m);
      }
    }

    m.setRecipient(recipient);
    dm.putOutgoing(m);
  }


  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    Map<String, Integer> temp = StaticSerialization.readHashMap(in, context);
    if (temp != null && !temp.isEmpty()) {
      gatewayMap = temp;
    }

  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    if (gatewayMap != null && !gatewayMap.isEmpty()) {
      StaticSerialization.writeHashMap(gatewayMap, out, context);
    }
  }

  @Override
  protected StringBuilder getStringBuilder() {
    StringBuilder sb = super.getStringBuilder();
    if (gatewayMap != null) {
      sb.append(" gatewayMap=");
      sb.append(gatewayMap);
    }
    return sb;
  }

}
