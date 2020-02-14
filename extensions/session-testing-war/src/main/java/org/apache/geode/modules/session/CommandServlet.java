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

package org.apache.geode.modules.session;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.apache.catalina.Manager;
import org.apache.catalina.session.StandardSession;
import org.apache.catalina.session.StandardSessionFacade;
import org.awaitility.Duration;

import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.modules.session.catalina.DeltaSessionManager;
import org.apache.geode.test.awaitility.GeodeAwaitility;

public class CommandServlet extends HttpServlet {
  @SuppressWarnings("unused")
  private ServletContext context;

  /**
   * Save a reference to the ServletContext for later use.
   */
  @Override
  public void init(ServletConfig config) {
    this.context = config.getServletContext();
  }

  /**
   * The standard servlet method overridden.
   */
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {

    QueryCommand cmd = QueryCommand.UNKNOWN;
    String param = request.getParameter("param");
    String value = request.getParameter("value");
    PrintWriter out = response.getWriter();

    try {
      String cmdStr = request.getParameter("cmd");
      if (cmdStr != null) {
        cmd = QueryCommand.valueOf(cmdStr);
      }

      HttpSession session;

      switch (cmd) {
        case SET:
          session = request.getSession();
          session.setAttribute(param, value);
          break;
        case SET_MAX_INACTIVE:
          session = request.getSession();
          session.setMaxInactiveInterval(Integer.valueOf(value));
          break;
        case GET:
          session = request.getSession();
          String val = (String) session.getAttribute(param);
          if (val != null) {
            out.write(val);
          }
          break;
        case REMOVE:
          session = request.getSession();
          session.removeAttribute(param);
          break;
        case INVALIDATE:
          session = request.getSession();
          session.invalidate();
          break;
        case FUNCTION:
          String functionClass = request.getParameter("function");
          Class<? extends Function> clazz = (Class<? extends Function>) Thread.currentThread()
              .getContextClassLoader().loadClass(functionClass);
          Function<HttpServletRequest, String> function = clazz.newInstance();
          String result = function.apply(request);
          if (result != null) {
            out.write(result);
          }
          break;
        case WAIT_UNTIL_QUEUE_DRAINED:
          session = request.getSession();
          DeltaSessionManager manager = (DeltaSessionManager) getSessionManager(session);
          GeodeAwaitility.await().pollInterval(Duration.TWO_HUNDRED_MILLISECONDS)
              .untilAsserted(() -> assertThat(checkQueueDrained(manager)).isTrue());
          break;
      }
    } catch (Exception e) {
      out.write("Error in servlet: " + e.toString());
      e.printStackTrace(out);
    }
  }

  private boolean checkQueueDrained(DeltaSessionManager manager) {
    GemFireCache cache = manager.getSessionCache().getCache();
    Execution execution = FunctionService.onServers(cache);

    ResultCollector collector = execution.execute(GetQueueSize.ID);
    List list = (List) collector.getResult();
    for (Object object : list) {
      for (Object queue : ((Map) object).keySet()) {
        manager.getLogger().info("client cache has queue: " + queue);
      }
    }
    for (Object object : list) {
      for (Object size : ((Map) object).values()) {
        if ((Integer) size != 0) {
          manager.getLogger().info("checkQueueDrained not drained with size " + size);
          return false;
        }
      }
    }
    manager.getLogger().info("checkQueueDrained drained");
    return true;
  }

  private Manager getSessionManager(HttpSession session) throws Exception {
    Field facadeSessionField = StandardSessionFacade.class.getDeclaredField("session");
    facadeSessionField.setAccessible(true);
    StandardSession stdSession = (StandardSession) facadeSessionField.get(session);

    return stdSession.getManager();
  }
}
