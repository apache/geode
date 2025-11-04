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

import java.io.File;

import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.core.StandardContext;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardWrapper;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.realm.MemoryRealm;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import org.apache.geode.modules.session.catalina.JvmRouteBinderValve;

/**
 * Embedded Tomcat 10+ server for testing session management.
 * Migrated from deprecated Embedded API to Tomcat 10 programmatic API.
 */
public class EmbeddedTomcat {
  private final Log logger = LogFactory.getLog(getClass());
  private final int port;
  private final Tomcat tomcat;
  private final Context rootContext;


  EmbeddedTomcat(int port, String jvmRoute) {
    this.port = port;

    // Create Tomcat instance using programmatic API (Tomcat 10+)
    tomcat = new Tomcat();

    // Set base directory for Tomcat
    File baseDir = new File("tomcat");
    baseDir.mkdirs();
    tomcat.setBaseDir(baseDir.getAbsolutePath());
    tomcat.setPort(port);
    tomcat.getHost().setAppBase(baseDir.getAbsolutePath());

    // Set hostname
    tomcat.setHostname("127.0.0.1");

    // Configure the engine with JVM route
    Engine engine = tomcat.getEngine();
    engine.setName("localEngine");
    engine.setJvmRoute(jvmRoute);

    // Set realm
    engine.setRealm(new MemoryRealm());

    // Create web application context
    String contextPath = "";
    String docBase = new File("").getAbsolutePath();
    rootContext = tomcat.addContext(contextPath, docBase);

    // Configure webapp loader - In Tomcat 10+, WebappLoader() no longer takes ClassLoader
    // Instead, we set the parent class loader after construction
    WebappLoader loader = new WebappLoader();
    loader.setLoaderClass(getClass().getClassLoader().getClass().getName());
    rootContext.setLoader(loader);

    // Configure context
    if (rootContext instanceof StandardContext) {
      StandardContext stdContext = (StandardContext) rootContext;
      stdContext.setReloadable(true);
      stdContext.setIgnoreAnnotations(true);
      stdContext.setParentClassLoader(getClass().getClassLoader());

      // In Tomcat 10+, repositories are managed differently
      // The classes directory will be found automatically via the context docBase
    }

    // Add JVMRoute valve for session failover
    ValveBase valve = new JvmRouteBinderValve();
    if (engine instanceof StandardEngine) {
      ((StandardEngine) engine).addValve(valve);
    }
  }

  /**
   * Starts the embedded Tomcat server.
   */
  void startContainer() throws LifecycleException {
    // Start Tomcat using the programmatic API
    tomcat.start();

    // add shutdown hook to stop server
    Runtime.getRuntime().addShutdownHook(new Thread(this::stopContainer));
  }

  /**
   * Stops the embedded Tomcat server.
   */
  void stopContainer() {
    try {
      if (tomcat != null && tomcat.getServer() != null) {
        tomcat.stop();
        tomcat.destroy();
        logger.info("Stopped container");
      }
    } catch (LifecycleException exception) {
      logger.warn("Cannot Stop Tomcat: " + exception.getMessage());
    }
  }

  StandardWrapper addServlet(String path, String name, String clazz) {
    // Use Tomcat's addServlet helper method (Tomcat 10+ API)
    // This automatically creates the wrapper and adds it to the context
    tomcat.addServlet(rootContext.getPath(), name, clazz);

    // Get the servlet that was just added
    StandardWrapper servlet = (StandardWrapper) rootContext.findChild(name);
    servlet.setLoadOnStartup(1);
    servlet.addMapping(path);

    return servlet;
  }

  /**
   * Gets the Tomcat instance.
   * Migrated from getEmbedded() which returned deprecated Embedded class.
   *
   * @return the Tomcat instance
   */
  Tomcat getTomcat() {
    return tomcat;
  }

  /**
   * @deprecated Use {@link #getTomcat()} instead.
   *             This method is maintained for backward compatibility.
   */
  @Deprecated
  Tomcat getEmbedded() {
    return tomcat;
  }

  Context getRootContext() {
    return rootContext;
  }

  public int getPort() {
    return port;
  }
}
