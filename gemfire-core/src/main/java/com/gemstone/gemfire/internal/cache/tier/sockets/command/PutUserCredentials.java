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
import com.gemstone.gemfire.internal.cache.tier.sockets.ServerConnection;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.security.GemFireSecurityException;

public class PutUserCredentials extends BaseCommand {
  
  private final static PutUserCredentials singleton = new PutUserCredentials();

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

      int numberOfParts = msg.getNumberOfParts();

      if (numberOfParts == 1) {
        // need to get credentials
        try {
          servConn.setAsTrue(REQUIRES_RESPONSE);
          byte[] uniqueId = servConn.setCredentials(msg);
          writeResponse(uniqueId, null, msg, false, servConn);
        } catch (GemFireSecurityException gfse) {
          if (servConn.getSecurityLogWriter().warningEnabled()) {
            servConn.getSecurityLogWriter().warning(LocalizedStrings.ONE_ARG,
                servConn.getName() + ": Security exception: " + gfse.toString()
                + (gfse.getCause() != null ? ", caused by: "
                    + gfse.getCause().toString() : ""));
          }
          writeException(msg, gfse, false, servConn);
        } catch (Exception ex) {
          if (servConn.getLogWriter().warningEnabled()) {
            servConn.getLogWriter().warning(LocalizedStrings
                .CacheClientNotifier_AN_EXCEPTION_WAS_THROWN_FOR_CLIENT_0_1,
                    new Object[] {servConn.getProxyID(), ""}, ex);
          }
          writeException(msg, ex, false, servConn);
        } finally {
          servConn.setAsTrue(RESPONDED);
        }

      } else {
        // need to throw some exeception
      }
    } else {
      // need to throw exception
    }
  }

}
