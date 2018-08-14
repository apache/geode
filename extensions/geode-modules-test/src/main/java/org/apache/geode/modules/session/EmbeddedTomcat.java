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
import java.net.InetAddress;
import java.net.MalformedURLException;

import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.Valve;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.core.StandardWrapper;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.realm.MemoryRealm;
import org.apache.catalina.startup.Embedded;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import org.apache.geode.modules.session.catalina.JvmRouteBinderValve;

public class EmbeddedTomcat {

  private String contextPath = null;
  private Embedded container = null;
  private Log logger = LogFactory.getLog(getClass());

  /**
   * The directory to create the Tomcat server configuration under.
   */
  private String catalinaHome = "tomcat";

  /**
   * The port to run the Tomcat server on.
   */
  private int port = 8089;

  /**
   * The classes directory for the web application being run.
   */
  private String classesDir = "target/classes";

  private Context rootContext = null;

  private Engine engine;

  /**
   * The web resources directory for the web application being run.
   */
  private String webappDir = "";

  public EmbeddedTomcat(String contextPath, int port, String jvmRoute)
      throws MalformedURLException {
    this.contextPath = contextPath;
    this.port = port;

    // create server
    container = new Embedded();
    container.setCatalinaHome(catalinaHome);

    container.setRealm(new MemoryRealm());

    // create webapp loader
    WebappLoader loader = new WebappLoader(this.getClass().getClassLoader());
    if (classesDir != null) {
      loader.addRepository(new File(classesDir).toURI().toURL().toString());
    }

    rootContext = container.createContext("", webappDir);
    rootContext.setLoader(loader);
    rootContext.setReloadable(true);
    // Otherwise we get NPE when instantiating servlets
    rootContext.setIgnoreAnnotations(true);

    // create host
    Host localHost = container.createHost("127.0.0.1", new File("").getAbsolutePath());
    localHost.addChild(rootContext);

    localHost.setDeployOnStartup(true);

    // create engine
    engine = container.createEngine();
    engine.setName("localEngine");
    engine.addChild(localHost);
    engine.setDefaultHost(localHost.getName());
    engine.setJvmRoute(jvmRoute);
    engine.setService(new StandardService());
    container.addEngine(engine);

    // create http connector
    Connector httpConnector = container.createConnector((InetAddress) null, port, false);
    container.addConnector(httpConnector);
    container.setAwait(true);

    // Create the JVMRoute valve for session failover
    ValveBase valve = new JvmRouteBinderValve();
    ((StandardEngine) engine).addValve(valve);
  }

  /**
   * Starts the embedded Tomcat server.
   */
  public void startContainer() throws LifecycleException {
    // start server
    container.start();

    // add shutdown hook to stop server
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        stopContainer();
      }
    });
  }

  /**
   * Stops the embedded Tomcat server.
   */
  public void stopContainer() {
    try {
      if (container != null) {
        container.stop();
        logger.info("Stopped container");
      }
    } catch (LifecycleException exception) {
      logger.warn("Cannot Stop Tomcat" + exception.getMessage());
    }
  }

  public StandardWrapper addServlet(String path, String name, String clazz)
      throws ServletException {
    StandardWrapper servlet = (StandardWrapper) rootContext.createWrapper();
    servlet.setName(name);
    servlet.setServletClass(clazz);
    servlet.setLoadOnStartup(1);

    rootContext.addChild(servlet);
    rootContext.addServletMapping(path, name);

    servlet.setParent(rootContext);
    // servlet.load();

    return servlet;
  }

  public Embedded getEmbedded() {
    return container;
  }

  public Context getRootContext() {
    return rootContext;
  }

  public String getPath() {
    return contextPath;
  }

  public void setPath(String path) {
    this.contextPath = path;
  }

  public int getPort() {
    return port;
  }

  public void addValve(Valve valve) {
    ((StandardEngine) engine).addValve(valve);
  }

  public void removeValve(Valve valve) {
    ((StandardEngine) engine).removeValve(valve);
  }
}
