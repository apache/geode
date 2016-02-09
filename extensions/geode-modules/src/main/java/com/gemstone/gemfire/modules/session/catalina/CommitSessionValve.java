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
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import javax.servlet.ServletException;
import java.io.IOException;

public class CommitSessionValve extends ValveBase {

  private static final Log log = LogFactory.getLog(CommitSessionValve.class);

  protected static final String info = "com.gemstone.gemfire.modules.session.catalina.CommitSessionValve/1.0";

  public CommitSessionValve() {
    log.info("Initialized");
  }

  @Override
  public void invoke(Request request, Response response) throws IOException, ServletException {
    // Get the Manager
    Manager manager = request.getContext().getManager();
    DeltaSessionFacade session = null;

    // Invoke the next Valve
    try {
      getNext().invoke(request, response);
    } finally {
      // Commit and if the correct Manager was found
      if (manager instanceof DeltaSessionManager) {
        session = (DeltaSessionFacade) request.getSession(false);
        if (session != null) {
          if (session.isValid()) {
            ((DeltaSessionManager) manager).removeTouchedSession(session.getId());
            session.commit();
            if (manager.getContainer().getLogger().isDebugEnabled()) {
              manager.getContainer().getLogger().debug(session + ": Committed.");
            }
          } else {
            if (manager.getContainer().getLogger().isDebugEnabled()) {
              manager.getContainer().getLogger().debug(session + ": Not valid so not committing.");
            }
          }
        }
      }
    }
  }
}
