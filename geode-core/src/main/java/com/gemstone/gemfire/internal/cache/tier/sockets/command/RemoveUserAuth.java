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
    
    if (!isSecureMode){
     // need to throw exception
     return;
    }

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
  }

}
