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

import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
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
  private final Log logger = LogFactory.getLog(getClass());
  private final int port;
  private final Embedded container;
  private final Context rootContext;

  EmbeddedTomcat(int port, String jvmRoute) throws MalformedURLException {
    this.port = port;

    // create server
    container = new Embedded();

    // The directory to create the Tomcat server configuration under.
    container.setCatalinaHome("tomcat");
    container.setRealm(new MemoryRealm());

    // create webapp loader
    WebappLoader loader = new WebappLoader(getClass().getClassLoader());
    // The classes directory for the web application being run.
    loader.addRepository(new File("target/classes").toURI().toURL().toString());

    // The web resources directory for the web application being run.
    String webappDir = "";
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
    Engine engine = container.createEngine();
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
  void startContainer() throws LifecycleException {
    // start server
    container.start();

    // add shutdown hook to stop server
    Runtime.getRuntime().addShutdownHook(new Thread(this::stopContainer));
  }

  /**
   * Stops the embedded Tomcat server.
   */
  void stopContainer() {
    try {
      if (container != null) {
        container.stop();
        logger.info("Stopped container");
      }
    } catch (LifecycleException exception) {
      logger.warn("Cannot Stop Tomcat" + exception.getMessage());
    }
  }

  StandardWrapper addServlet(String path, String name, String clazz) {
    StandardWrapper servlet = (StandardWrapper) rootContext.createWrapper();
    servlet.setName(name);
    servlet.setServletClass(clazz);
    servlet.setLoadOnStartup(1);

    rootContext.addChild(servlet);
    rootContext.addServletMapping(path, name);

    servlet.setParent(rootContext);

    return servlet;
  }

  Embedded getEmbedded() {
    return container;
  }

  Context getRootContext() {
    return rootContext;
  }

  public int getPort() {
    return port;
  }
}
