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
package com.gemstone.gemfire.modules.session;

import java.io.File;
import java.net.MalformedURLException;

import javax.security.auth.message.config.AuthConfigFactory;
import javax.servlet.ServletException;

import org.apache.catalina.Context;
import org.apache.catalina.Engine;
import org.apache.catalina.Host;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.LifecycleListener;
import org.apache.catalina.Valve;
import org.apache.catalina.authenticator.jaspic.AuthConfigFactoryImpl;
import org.apache.catalina.authenticator.jaspic.SimpleAuthConfigProvider;
import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardEngine;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.core.StandardService;
import org.apache.catalina.core.StandardWrapper;
import org.apache.catalina.loader.WebappLoader;
import org.apache.catalina.startup.Tomcat;
import org.apache.catalina.valves.ValveBase;
import org.apache.juli.logging.Log;
import org.apache.juli.logging.LogFactory;

import com.gemstone.gemfire.modules.session.catalina.JvmRouteBinderValve;

/**
 *
 */
public class EmbeddedTomcat8 {

  private String contextPath = null;
  private Tomcat container = null;
  private Log logger = LogFactory.getLog(getClass());

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

  public EmbeddedTomcat8(String contextPath, int port, String jvmRoute) throws MalformedURLException {
    this.contextPath = contextPath;
    this.port = port;

    // create server
    container = new Tomcat();
    container.setBaseDir(System.getProperty("user.dir") + "/tomcat");

    Host localHost = container.getHost();//("127.0.0.1", new File("").getAbsolutePath());
    localHost.setDeployOnStartup(true);
    localHost.getCreateDirs();

    try {
      new File(localHost.getAppBaseFile().getAbsolutePath()).mkdir();
      new File(localHost.getCatalinaBase().getAbsolutePath(), "logs").mkdir();
      rootContext = container.addContext("", localHost.getAppBaseFile().getAbsolutePath());
    }
    catch (Exception e) {
      throw new Error(e);
    }
    // Otherwise we get NPE when instantiating servlets
    rootContext.setIgnoreAnnotations(true);


    AuthConfigFactory factory = new AuthConfigFactoryImpl();
    new SimpleAuthConfigProvider(null, factory);
    AuthConfigFactory.setFactory(factory);

    // create engine
    engine = container.getEngine();
    engine.setName("localEngine");
    engine.setJvmRoute(jvmRoute);

    // create http connector
    container.setPort(port);
    Connector httpConnector = container.getConnector();//((InetAddress) null, port, false);

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

  public StandardWrapper addServlet(String path, String name, String clazz) throws ServletException {
    StandardWrapper servlet = (StandardWrapper) rootContext.createWrapper();
    servlet.setName(name);
    servlet.setServletClass(clazz);
    servlet.setLoadOnStartup(1);

    rootContext.addChild(servlet);
    rootContext.addServletMapping(path, name);

    servlet.setParent(rootContext);
//        servlet.load();

    return servlet;
  }

  public Tomcat getEmbedded() {
    return container;
  }

  public void addLifecycleListener(LifecycleListener lifecycleListener) {
    container.getServer().addLifecycleListener(lifecycleListener);
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

}
