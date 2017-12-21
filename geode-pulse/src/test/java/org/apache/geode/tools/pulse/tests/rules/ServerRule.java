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
package org.apache.geode.tools.pulse.tests.rules;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.awaitility.Awaitility;
import org.junit.rules.ExternalResource;

import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.admin.SSLConfig;
import org.apache.geode.internal.net.SocketCreatorFactory;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.management.internal.JettyHelper;
import org.apache.geode.tools.pulse.internal.data.PulseConstants;
import org.apache.geode.tools.pulse.tests.Server;

public class ServerRule extends ExternalResource {
  private static final String LOCALHOST = "localhost";
  private static final String PULSE_CONTEXT = "/pulse/";

  private org.eclipse.jetty.server.Server jetty;
  private Server server;
  private String pulseURL;

  public ServerRule(String jsonAuthFile) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    String jmxPropertiesFile = classLoader.getResource("test.properties").getPath();

    int jmxPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    server = Server.createServer(jmxPort, jmxPropertiesFile, jsonAuthFile);
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_HOST, LOCALHOST);
    System.setProperty(PulseConstants.SYSTEM_PROPERTY_PULSE_PORT, Integer.toString(jmxPort));

    SocketCreatorFactory.setDistributionConfig(new DistributionConfigImpl(new Properties()));
    int httpPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    jetty = JettyHelper.initJetty(LOCALHOST, httpPort, SecurableCommunicationChannel.WEB);
    JettyHelper.addWebApplication(jetty, PULSE_CONTEXT, getPulseWarPath());
    pulseURL = "http://" + LOCALHOST + ":" + httpPort + PULSE_CONTEXT;
    System.out.println("Pulse started at " + pulseURL);
  }

  public String getPulseURL() {
    return this.pulseURL;
  }


  @Override
  protected void before() throws Throwable {
    jetty.start();
    Awaitility.await().until(() -> jetty.isStarted());
  }

  @Override
  protected void after() {
    try {
      stopJetty();
    } finally {
      stopServer();
    }
  }

  private void stopServer() {
    try {
      if (server != null) {
        server.stop();
      }
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private void stopJetty() {
    try {
      if (jetty != null) {
        jetty.stop();
        jetty = null;
      }
    } catch (Exception e) {
      throw new Error(e);
    }
  }

  private String getPulseWarPath() {
    String warPath = null;
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    InputStream inputStream = classLoader.getResourceAsStream("GemFireVersion.properties");
    Properties properties = new Properties();
    try {
      properties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Unable to open properties file", e);
    }
    String version = properties.getProperty("Product-Version");
    warPath = "geode-pulse-" + version + ".war";
    String propFilePath = classLoader.getResource("GemFireVersion.properties").getPath();
    warPath =
        propFilePath.substring(0, propFilePath.indexOf("generated-resources")) + "libs/" + warPath;
    return warPath;
  }

}
