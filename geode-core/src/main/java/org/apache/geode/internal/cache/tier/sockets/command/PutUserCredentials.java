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
package org.apache.geode.internal.cache.tier.sockets.command;

import java.io.IOException;

import org.apache.geode.internal.cache.tier.Command;
import org.apache.geode.internal.cache.tier.sockets.BaseCommand;
import org.apache.geode.internal.cache.tier.sockets.Message;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.security.GemFireSecurityException;

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
