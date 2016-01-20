/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
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
