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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.internal.cache.CachedDeserializable;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.ha.HAContainerWrapper;
import org.apache.geode.internal.cache.tier.CachedRegionHelper;
import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.CacheClientNotifier;
import org.apache.geode.internal.cache.tier.sockets.CacheServerHelper;
import org.apache.geode.internal.cache.tier.sockets.ClientUpdateMessageImpl;
import org.apache.geode.internal.cache.tier.sockets.HAEventWrapper;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.Part;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.security.SecurityService;

/**
 * Represents a request for (full) value of a given event from ha container
 * (client-messages-region).
 *
 * @since GemFire 6.1
 */
public class RequestEventValue extends BaseCommand {

  private static final RequestEventValue singleton = new RequestEventValue();

  public static Command getCommand() {
    return singleton;
  }

  private RequestEventValue() {}

  public void cmdExecute(final Message clientMessage, final ServerConnection serverConnection,
      final SecurityService securityService, long start) throws IOException {
    Part eventIDPart = null, valuePart = null;
    EventID event = null;
    Object callbackArg = null;
    CachedRegionHelper crHelper = serverConnection.getCachedRegionHelper();
    StringBuffer errMessage = new StringBuffer();

    serverConnection.setAsTrue(REQUIRES_RESPONSE);

    // Retrieve the data from the message parts
    int parts = clientMessage.getNumberOfParts();
    eventIDPart = clientMessage.getPart(0);

    if (eventIDPart == null) {
      logger.warn("{}: The event id for the get event value request is null.",
          serverConnection.getName());
      errMessage.append(" The event id for the get event value request is null.");
      writeErrorResponse(clientMessage, MessageType.REQUESTDATAERROR, errMessage.toString(),
          serverConnection);
      serverConnection.setAsTrue(RESPONDED);
    } else {
      try {
        event = (EventID) eventIDPart.getObject();
      } catch (Exception e) {
        writeException(clientMessage, e, false, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        return;
      }
      if (parts > 1) {
        valuePart = clientMessage.getPart(1);
        try {
          if (valuePart != null) {
            callbackArg = valuePart.getObject();
          }
        } catch (Exception e) {
          writeException(clientMessage, e, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }
      }
      if (logger.isTraceEnabled()) {
        logger.trace("{}: Received get event value request ({} bytes) from {}",
            serverConnection.getName(), clientMessage.getPayloadLength(),
            serverConnection.getSocketString());
      }
      CacheClientNotifier ccn = serverConnection.getAcceptor().getCacheClientNotifier();
      // Get the ha container.
      HAContainerWrapper haContainer = (HAContainerWrapper) ccn.getHaContainer();
      if (haContainer == null) {
        String reason = " was not found during get event value request";
        writeRegionDestroyedEx(clientMessage, "ha container", reason, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
      } else {
        Object[] valueAndIsObject = new Object[2];
        try {
          Object data = haContainer.get(new HAEventWrapper(event));

          if (data == null) {
            logger.warn("Unable to find a client update message for {}",
                event);
            String msgStr = "No value found for " + event + " in " + haContainer.getName();
            writeErrorResponse(clientMessage, MessageType.REQUEST_EVENT_VALUE_ERROR, msgStr,
                serverConnection);
            serverConnection.setAsTrue(RESPONDED);
            return;
          } else {
            if (logger.isDebugEnabled()) {
              logger.debug("Value retrieved for event {}", event);
            }
            Object val = ((ClientUpdateMessageImpl) data).getValueToConflate();
            if (!(val instanceof byte[])) {
              if (val instanceof CachedDeserializable) {
                val = ((CachedDeserializable) val).getSerializedValue();
              } else {
                val = CacheServerHelper.serialize(val);
              }
              ((ClientUpdateMessageImpl) data).setLatestValue(val);
            }
            valueAndIsObject[0] = val;
            valueAndIsObject[1] = Boolean.valueOf(((ClientUpdateMessageImpl) data).valueIsObject());
          }
        } catch (Exception e) {
          writeException(clientMessage, e, false, serverConnection);
          serverConnection.setAsTrue(RESPONDED);
          return;
        }

        Object data = valueAndIsObject[0];
        boolean isObject = (Boolean) valueAndIsObject[1];

        writeResponse(data, callbackArg, clientMessage, isObject, serverConnection);
        serverConnection.setAsTrue(RESPONDED);
        ccn.getClientProxy(serverConnection.getProxyID()).getStatistics()
            .incDeltaFullMessagesSent();
        if (logger.isDebugEnabled()) {
          logger.debug("{}: Wrote get event value response back to {} for ha container {}",
              serverConnection.getName(), serverConnection.getSocketString(),
              haContainer.getName());
        }
      }
    }
  }
}
