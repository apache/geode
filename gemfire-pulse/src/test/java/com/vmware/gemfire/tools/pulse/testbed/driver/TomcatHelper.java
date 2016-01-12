/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.testbed.driver;

import java.net.InetAddress;

import org.apache.catalina.connector.Connector;
import org.apache.catalina.core.StandardHost;
import org.apache.catalina.startup.Tomcat;
import org.apache.tomcat.util.IntrospectionUtils;

public class TomcatHelper {
  public static Tomcat startTomcat(String bindAddress, int port, String context, String path) throws Exception {

    Tomcat tomcat = new Tomcat();

    // Set up logging - first we're going to remove all existing handlers. Don't do this before Tomcat is
    // instantiated otherwise there isn't anything to remove.
    /*

    Logger globalLogger = Logger.getLogger("");
    for (Handler handler : globalLogger.getHandlers()) {
      globalLogger.removeHandler(handler);
    }

    // Now let's add our handler
    Handler gfHandler = new GemFireHandler((LogWriterImpl) log);
    Logger logger = Logger.getLogger("");
    logger.addHandler(gfHandler);
    
    */

    // Set up for commons-logging which is used by Spring.
    // This forces JCL to use the JDK logger otherwise it defaults to trying to use Log4J.
    if (System.getProperty("org.apache.commons.logging.Log") == null) {
      System.setProperty("org.apache.commons.logging.Log", "org.apache.commons.logging.impl.Jdk14Logger");
    }

    if (bindAddress != null && bindAddress.length() > 0) {
      Connector c = tomcat.getConnector();
      IntrospectionUtils.setProperty(c, "address", bindAddress);
    }
    tomcat.setPort(port);

    // Working (scratch) dir
    String scratch = System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "Pulse_"
        + ((bindAddress == null || bindAddress.length() == 0) ? "0.0.0.0" : bindAddress) + "_" + port + "_"
        + String.format("%x", path.hashCode());
    
    
    tomcat.setBaseDir(scratch);
    StandardHost stdHost = (StandardHost) tomcat.getHost();
    //stdHost.setUnpackWARs(false);   
    //tomcat.addContext(context, path);
    tomcat.addWebapp(stdHost, context, path);
    stdHost.setDeployOnStartup(true);    
    tomcat.start();

    return tomcat;
  }
}
