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

package org.apache.geode.modules.session.internal.filter;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;

import jakarta.servlet.DispatcherType;
import org.eclipse.jetty.ee10.servlet.FilterHolder;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.LocalConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Embedded Jetty test server for servlet and filter integration testing.
 *
 * <p>
 * <b>Jakarta EE 10 Migration:</b> This class was completely rewritten for Jetty 12 compatibility.
 *
 * <p>
 * <b>Original Implementation (pre-migration):</b>
 * <ul>
 * <li>Extended Jetty's {@code ServletTester} class (removed in Jetty 12)</li>
 * <li>Relied on package-private {@code _server} variable access</li>
 * <li>Provided simple {@code isStarted()} and {@code isStopped()} wrapper methods</li>
 * </ul>
 *
 * <p>
 * <b>Current Implementation (Jetty 12):</b>
 * <ul>
 * <li>Standalone class using Jetty 12's embedded server API</li>
 * <li>Uses {@link Server}, {@link ServletContextHandler}, {@link LocalConnector} for testing</li>
 * <li>Provides full servlet/filter registration and HTTP request/response handling</li>
 * <li>Maintains backward compatibility with existing test code</li>
 * </ul>
 *
 * <p>
 * <b>Why the rewrite:</b> Jetty 12 removed {@code ServletTester} class entirely, requiring
 * a custom implementation using the new embedded server APIs to maintain test functionality.
 */
public class MyServletTester {
  private Server server;
  private ServletContextHandler context;
  private LocalConnector localConnector;
  private ServerConnector serverConnector;
  private String contextPath = "/";
  private boolean useSecure = false;

  public MyServletTester() {
    server = new Server();
    localConnector = new LocalConnector(server);
    server.addConnector(localConnector);

    context = new ServletContextHandler(ServletContextHandler.SESSIONS);
    context.setContextPath(contextPath);
    server.setHandler(context);
  }

  public boolean isStarted() {
    return server != null && server.isStarted();
  }

  public boolean isStopped() {
    return server != null && server.isStopped();
  }

  public void setContextPath(String path) {
    this.contextPath = path;
    context.setContextPath(path);
  }

  public FilterHolder addFilter(Class<?> filterClass, String pathSpec,
      EnumSet<DispatcherType> dispatches) {
    @SuppressWarnings("unchecked")
    Class<? extends jakarta.servlet.Filter> fc =
        (Class<? extends jakarta.servlet.Filter>) filterClass;
    FilterHolder holder = new FilterHolder(fc);
    context.addFilter(holder, pathSpec, dispatches);
    return holder;
  }

  public ServletHolder addServlet(String className, String pathSpec) {
    try {
      Class<?> servletClass = Class.forName(className);
      @SuppressWarnings("unchecked")
      Class<? extends jakarta.servlet.Servlet> sc =
          (Class<? extends jakarta.servlet.Servlet>) servletClass;
      ServletHolder holder = new ServletHolder(sc);
      context.addServlet(holder, pathSpec);
      return holder;
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("Failed to load servlet class: " + className, e);
    }
  }

  public void setAttribute(String name, Object value) {
    context.setAttribute(name, value);
  }

  public void stop() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  public String getResponses(ByteBuffer request) throws Exception {
    String requestString = StandardCharsets.UTF_8.decode(request).toString();
    return localConnector.getResponse(requestString);
  }

  public String createConnector(boolean secure) {
    // Create a ServerConnector for real HTTP connections (needed by HttpUnit tests)
    // Note: The 'secure' parameter is ignored - we only support HTTP for these tests
    // The old Jetty ServletTester also didn't actually support HTTPS
    if (serverConnector == null) {
      this.useSecure = false; // Always use HTTP
      serverConnector = new ServerConnector(server);
      serverConnector.setPort(0); // Use any available port
      server.addConnector(serverConnector);

      // Pre-open the connector to get the port - this is what the old ServletTester did
      try {
        serverConnector.open();
        int port = serverConnector.getLocalPort();
        return "http://localhost:" + port;
      } catch (Exception e) {
        throw new RuntimeException("Failed to open connector", e);
      }
    }

    // If connector already exists, return the URL
    int port = serverConnector.getLocalPort();
    return "http://localhost:" + port;
  }

  public void start() throws Exception {
    server.start();
  }

  public String getConnectorUrl() {
    if (serverConnector != null) {
      int port = serverConnector.getLocalPort();
      return (useSecure ? "https" : "http") + "://localhost:" + port;
    }
    return null;
  }

  public void setResourceBase(String path) {
    context.setBaseResourceAsString(path);
  }

  public Context getContext() {
    return new Context(context);
  }

  /**
   * Wrapper for ServletContextHandler to provide compatibility with old API
   */
  public static class Context {
    private final ServletContextHandler handler;

    public Context(ServletContextHandler handler) {
      this.handler = handler;
    }

    public void setClassLoader(ClassLoader classLoader) {
      handler.setClassLoader(classLoader);
    }
  }
}
