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
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.internal.cache.CachedDeserializable;
import com.gemstone.gemfire.internal.cache.EventID;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.ha.HAContainerWrapper;
import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheClientNotifier;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import com.gemstone.gemfire.internal.cache.tier.sockets.HAEventWrapper;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.log4j.LocalizedMessage;

/**
 * Represents a request for (full) value of a given event from ha container
 * (client-messages-region).
 * 
 * @since GemFire 6.1
 */
public class RequestEventValue extends BaseCommand {

  private final static RequestEventValue singleton = new RequestEventValue();

  public static Command getCommand() {
    return singleton;
  }

  private RequestEventValue() {
  }

  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    Part eventIDPart = null, valuePart = null;
    EventID event = null;
    Object callbackArg = null;
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    StringBuffer errMessage = new StringBuffer();

    // TODO: Amogh- Do we need to keep this?
    if (crHelper.emulateSlowServer() > 0) {
      boolean interrupted = Thread.interrupted();
      try {
        Thread.sleep(crHelper.emulateSlowServer());
      }
      catch (InterruptedException ugh) {
        interrupted = true;
        ((GemFireCacheImpl)(servConn.getCachedRegionHelper().getCache()))
            .getCancelCriterion().checkCancelInProgress(ugh);
      }
      finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
    servConn.setAsTrue(REQUIRES_RESPONSE);

    // Retrieve the data from the message parts
    int parts = msg.getNumberOfParts();
    eventIDPart = msg.getPart(0);
    
    if (eventIDPart == null) {
      logger.warn(LocalizedMessage.create(LocalizedStrings.RequestEventValue_0_THE_EVENT_ID_FOR_THE_GET_EVENT_VALUE_REQUEST_IS_NULL, servConn.getName()));
      errMessage
          .append(" The event id for the get event value request is null.");
      writeErrorResponse(msg, MessageType.REQUESTDATAERROR, errMessage
          .toString(), servConn);
      servConn.setAsTrue(RESPONDED);
    }
    else {
      try {
        event = (EventID)eventIDPart.getObject();
      }
      catch (Exception e) {
        writeException(msg, e, false, servConn);
        servConn.setAsTrue(RESPONDED);
        return;
      }
      if (parts > 1) {
        valuePart = msg.getPart(1);
        try {
          if (valuePart != null) {
            callbackArg = valuePart.getObject();
          }
        }
        catch (Exception e) {
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Received get event value request ({} bytes) from {}", servConn.getName(), msg.getPayloadLength(), servConn.getSocketString());
      }
      CacheClientNotifier ccn = servConn.getAcceptor().getCacheClientNotifier();
      // Get the ha container.
      HAContainerWrapper haContainer = (HAContainerWrapper)ccn.getHaContainer();
      if (haContainer == null) {
        String reason = " was not found during get event value request";
        writeRegionDestroyedEx(msg, "ha container", reason, servConn);
        servConn.setAsTrue(RESPONDED);
      }
      else {
        Object[] valueAndIsObject = new Object[2];
        try {
          Object data = haContainer.get(new HAEventWrapper(event));

          if (data == null) {
            logger.warn(LocalizedMessage.create(LocalizedStrings.RequestEventValue_UNABLE_TO_FIND_A_CLIENT_UPDATE_MESSAGE_FOR_0, event));
            String msgStr = "No value found for " + event + " in " + haContainer.getName();
            writeErrorResponse(msg, MessageType.REQUEST_EVENT_VALUE_ERROR, msgStr, servConn);
            servConn.setAsTrue(RESPONDED);
            return;
          }
          else {
            if (logger.isDebugEnabled()) {
              logger.debug("Value retrieved for event {}", event);
            }
            Object val = ((ClientUpdateMessageImpl)data).getValueToConflate();
            if (!(val instanceof byte[])) {
              if(val instanceof CachedDeserializable) {
                val = ((CachedDeserializable) val).getSerializedValue();
              } else {
                val = CacheServerHelper.serialize(val);
              }
              ((ClientUpdateMessageImpl)data).setLatestValue(val);
            }
            valueAndIsObject[0] = val;
            valueAndIsObject[1] = Boolean.valueOf(((ClientUpdateMessageImpl)data).valueIsObject());
          }
        }
        catch (Exception e) {
          writeException(msg, e, false, servConn);
          servConn.setAsTrue(RESPONDED);
          return;
        }

        Object data = valueAndIsObject[0];
        boolean isObject = (Boolean)valueAndIsObject[1];

        writeResponse(data, callbackArg, msg, isObject, servConn);
        servConn.setAsTrue(RESPONDED);
        ccn.getClientProxy(servConn.getProxyID()).getStatistics().incDeltaFullMessagesSent();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Wrote get event value response back to {} for ha container {}", servConn.getName(), servConn.getSocketString(), haContainer.getName());
        }
      }
    }
  }
}
