/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;

import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.BaseCommand;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.GemFireSecurityException;

public class RemoveUserAuth extends BaseCommand {

  private final static RemoveUserAuth singleton = new RemoveUserAuth();

  public static Command getCommand() {
    return singleton;
  }

  @Override
  public void cmdExecute(Message msg, ServerConnection servConn, long start)
      throws IOException, ClassNotFoundException, InterruptedException {
    boolean isSecureMode = msg.isSecureMode();
    
   // if (!isSecureMode)
     //client has not send secuirty header, need to send exception and log this in security (file)

    if (isSecureMode) {

      try {
        servConn.setAsTrue(REQUIRES_RESPONSE);
        Part keepalivePart = msg.getPart(0);
        byte[] keepaliveByte = keepalivePart.getSerializedForm();
        boolean keepalive = (keepaliveByte == null || keepaliveByte[0] == 0) ? false
            : true;
        servConn.getSecurityLogWriter().fine(
            "remove user auth keep alive " + keepalive);
        servConn.removeUserAuth(msg, keepalive);
        writeReply(msg, servConn);
      } catch (GemFireSecurityException gfse) {
        if (servConn.getSecurityLogWriter().warningEnabled()) {
          servConn.getSecurityLogWriter()
              .warning(
                  LocalizedStrings.ONE_ARG,
                  servConn.getName() + ": Security exception: "
                      + gfse.getMessage());
        }
        writeException(msg, gfse, false, servConn);
      } catch (Exception ex) {
        // TODO Auto-generated catch block
        if (servConn.getLogWriter().warningEnabled()) {
          servConn
              .getLogWriter()
              .warning(
                  LocalizedStrings.CacheClientNotifier_AN_EXCEPTION_WAS_THROWN_FOR_CLIENT_0_1,
                  new Object[] {servConn.getProxyID(), ""}, ex);
        }
        writeException(msg, ex, false, servConn);
      } finally {
        servConn.setAsTrue(RESPONDED);
      }
    } else {
      // need to throw exception
    }
  }

}
