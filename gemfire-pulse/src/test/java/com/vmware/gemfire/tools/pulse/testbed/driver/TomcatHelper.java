/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
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
