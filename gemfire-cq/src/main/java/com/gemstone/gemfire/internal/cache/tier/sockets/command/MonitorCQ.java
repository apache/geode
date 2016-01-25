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


import com.gemstone.gemfire.internal.cache.tier.CachedRegionHelper;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.MessageType;
import com.gemstone.gemfire.internal.cache.tier.sockets.*;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.cache.query.CqException;
import com.gemstone.gemfire.cache.query.internal.DefaultQueryService;
import com.gemstone.gemfire.cache.query.internal.cq.CqService;

import java.io.IOException;

public class MonitorCQ extends BaseCQCommand {

  private final static MonitorCQ singleton = new MonitorCQ();

  public static Command getCommand() {
    return singleton;
  }

  private MonitorCQ() {
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException {
    CachedRegionHelper crHelper = servConn.getCachedRegionHelper();
    servConn.setAsTrue(REQUIRES_RESPONSE);
    servConn.setAsTrue(REQUIRES_CHUNKED_RESPONSE);

    int op = msg.getPart(0).getInt();

    if (op < 1) {
      // This should have been taken care at the client - remove?
      String err = LocalizedStrings.MonitorCQ__0_THE_MONITORCQ_OPERATION_IS_INVALID.toLocalizedString(servConn.getName());
      sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
          .getTransactionId(), null, servConn);
      return;
    }

    String regionName = null;
    if (msg.getNumberOfParts() == 2) {
      // This will be enable/disable on region.
      regionName = msg.getPart(1).getString();
      if (regionName == null) {
        // This should have been taken care at the client - remove?
        String err = LocalizedStrings.MonitorCQ__0_A_NULL_REGION_NAME_WAS_PASSED_FOR_MONITORCQ_OPERATION.toLocalizedString(servConn.getName());
        sendCqResponse(MessageType.CQDATAERROR_MSG_TYPE, err, msg
            .getTransactionId(), null, servConn);
        return;
      }
    }

    if (logger.isDebugEnabled()) {
      logger.debug("{}: Received MonitorCq request from {} op: {}{}", servConn.getName(), servConn.getSocketString(), op, (regionName != null) ? " RegionName: " + regionName : "");
    }

    try {
      CqService cqService = crHelper.getCache().getCqService();
      cqService.start();
      // The implementation of enable/disable cq is changed.
      // Instead calling enable/disable client calls execute/stop methods
      // at cache and region level.
      // This method is retained for future purpose, to support admin level apis
      // similar to enable/disable at system/client level.
      // Should never come.
      throw new CqException (LocalizedStrings.CqService_INVALID_CQ_MONITOR_REQUEST_RECEIVED.toLocalizedString());
    }
    catch (CqException cqe) {
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, "", msg.getTransactionId(),
          cqe, servConn);
      return;
    }
    catch (Exception e) {
      String err = LocalizedStrings.MonitorCQ_EXCEPTION_WHILE_HANDLING_THE_MONITOR_REQUEST_OP_IS_0.toLocalizedString(Integer.valueOf(op));
      sendCqResponse(MessageType.CQ_EXCEPTION_TYPE, err,
          msg.getTransactionId(), e, servConn);
      return;
    }
  }

}
