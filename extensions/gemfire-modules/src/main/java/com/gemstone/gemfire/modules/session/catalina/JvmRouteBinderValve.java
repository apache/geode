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
package com.gemstone.gemfire.modules.session.catalina;

import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;

import javax.servlet.ServletException;
import java.io.IOException;

public class JvmRouteBinderValve extends ValveBase {

  protected static final String info = "com.gemstone.gemfire.modules.session.JvmRouteBinderValve/1.0";

  @Override
  public void invoke(Request request, Response response) throws IOException, ServletException {

    // Get the Manager
    Manager manager = request.getContext().getManager();

    // If it is an AbstractManager, handle possible failover
    if (manager instanceof DeltaSessionManager) {
      DeltaSessionManager absMgr = (DeltaSessionManager) manager;
      String localJvmRoute = absMgr.getJvmRoute();
      if (localJvmRoute != null) {
        handlePossibleFailover(request, absMgr, localJvmRoute);
      }
    }

    // Invoke the next Valve
    getNext().invoke(request, response);
  }

  private void handlePossibleFailover(Request request, DeltaSessionManager manager, String localJvmRoute) {
    String sessionId = request.getRequestedSessionId();
    if (sessionId != null) {
      // Get request JVM route
      String requestJvmRoute = null;
      int index = sessionId.indexOf(".");
      if (index > 0) {
        requestJvmRoute = sessionId.substring(index + 1, sessionId.length());
      }

      // If the requested JVM route doesn't equal the session's JVM route, handle failover
      if (requestJvmRoute != null && !requestJvmRoute.equals(localJvmRoute)) {
        if (manager.getLogger().isDebugEnabled()) {
          StringBuilder builder = new StringBuilder();
          builder.append(this)
              .append(": Handling failover of session ")
              .append(sessionId)
              .append(" from ")
              .append(requestJvmRoute)
              .append(" to ")
              .append(localJvmRoute);
          manager.getLogger().debug(builder.toString());
        }
        // Get the original session
        Session session = null;
        try {
          session = manager.findSession(sessionId);
        } catch (IOException e) {
          StringBuilder builder = new StringBuilder();
          builder.append(this)
              .append(": Caught exception attempting to find session ")
              .append(sessionId)
              .append(" in ")
              .append(manager);
          manager.getLogger().warn(builder.toString(), e);
        }

        if (session == null) {
          StringBuilder builder = new StringBuilder();
          builder.append(this)
              .append(": Did not find session ")
              .append(sessionId)
              .append(" to failover in ")
              .append(manager);
          manager.getLogger().warn(builder.toString());
        } else {
          // Change its session id. This removes the previous session and creates the new one.
          String baseSessionId = sessionId.substring(0, index);
          String newSessionId = baseSessionId + "." + localJvmRoute;
          session.setId(newSessionId);

          // Change the request's session id
          request.changeSessionId(newSessionId);
        }
      }
    }
  }
}
