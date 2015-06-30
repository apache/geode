/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session;

import com.gemstone.gemfire.modules.session.catalina.JvmRouteBinderValve;
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

/**
 *
 */
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
        // Not really necessasry, but let's still do it...
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
        ((StandardEngine)engine).addValve(valve);
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
