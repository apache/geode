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
package org.apache.geode.modules.session.catalina;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.Manager;
import org.apache.catalina.connector.Request;
import org.apache.catalina.connector.Response;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

public abstract class AbstractCommitSessionValve<SelfT extends AbstractCommitSessionValve<?>>
    extends ValveBase {

  private static final Log log = LogFactory.getLog(AbstractCommitSessionValve.class);

  protected static final String info =
      "org.apache.geode.modules.session.catalina.CommitSessionValve/1.0";

  AbstractCommitSessionValve() {
    log.info("Initialized");
  }

  @Override
  public void invoke(final Request request, final Response response)
      throws IOException, ServletException {
    try {
      getNext().invoke(request, wrapResponse(response));
    } finally {
      commitSession(request);
    }
  }

  /**
   * Commit session only if DeltaSessionManager is in place.
   *
   * @param request to commit session from.
   */
  protected static <SelfT extends AbstractCommitSessionValve<?>> void commitSession(
      final Request request) {
    final Context context = request.getContext();
    final Manager manager = context.getManager();
    if (manager instanceof DeltaSessionManager) {
      final DeltaSessionFacade session = (DeltaSessionFacade) request.getSession(false);
      if (session != null) {
        @SuppressWarnings("unchecked")
        final DeltaSessionManager<SelfT> deltaSessionManager = (DeltaSessionManager<SelfT>) manager;
        if (session.isValid()) {
          deltaSessionManager.removeTouchedSession(session.getId());
          session.commit();
          if (log.isDebugEnabled()) {
            log.debug(session + ": Committed.");
          }
        } else {
          if (log.isDebugEnabled()) {
            log.debug(session + ": Not valid so not committing.");
          }
        }
      }
    }
  }

  abstract Response wrapResponse(final Response response);

}
