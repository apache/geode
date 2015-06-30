/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.catalina;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public class CommitSessionValve extends ValveBase {
  
  private static final Log log = LogFactory.getLog(CommitSessionValve.class);

  protected static final String info = "com.gemstone.gemfire.modules.session.catalina.CommitSessionValve/1.0";

  public CommitSessionValve() {
    log.info("Initialized");
  }

  @Override
  public void invoke(Request request, Response response) throws IOException,
      ServletException {
    // Get the Manager
    Manager manager = request.getContext().getManager();
    DeltaSessionFacade session = null;
    
    // Invoke the next Valve
    try {
      getNext().invoke(request, response);
    } finally {
      // Commit and if the correct Manager was found
      if (manager instanceof DeltaSessionManager) {
        session = (DeltaSessionFacade)request.getSession(false);
        if (session != null) {
          if (session.isValid()) {
            ((DeltaSessionManager) manager).removeTouchedSession(session.getId());
            session.commit();
            if (manager.getContainer().getLogger().isDebugEnabled()) {
              manager.getContainer().getLogger().debug(session + ": Committed.");
            }
          } else {
            if (manager.getContainer().getLogger().isDebugEnabled()) {
              manager.getContainer().getLogger().debug(
                  session + ": Not valid so not committing.");
            }
          }
        }
      }
    }
  }
}
