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
package org.apache.geode.internal.net;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.SystemErrRule;
import org.junit.contrib.java.lang.system.SystemOutRule;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.distributed.ClientSocketFactory;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionConfigImpl;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.security.SecurableCommunicationChannel;
import org.apache.geode.test.dunit.ThreadUtils;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.util.test.TestUtil;

/**
 * Test creation of server sockets and client sockets with various JSSE
 * configurations.
 */
@Category(IntegrationTest.class)
public class JSSESocketJUnitTest {

  private static volatile boolean factoryInvoked;

  private ServerSocket acceptor;
  private Socket server;
  private int randport;

  @Rule
  public TestName name = new TestName();

  @Rule
  public SystemOutRule systemOutRule = new SystemOutRule();

  @Before
  public void setUp() throws Exception {
    randport = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  }

  @After
  public void tearDown() throws Exception {
    if (server != null) {
      server.close();
    }
    if (acceptor != null) {
      acceptor.close();
    }
    SocketCreatorFactory.close();
  }

  @Test
  public void testSSLSocket() throws Exception {
    systemOutRule.mute().enableLog();

    final Object[] receiver = new Object[1];

    // Get original base log level
    Level originalBaseLevel = LogService.getBaseLogLevel();
    try {
      // Set base log level to debug to log the SSL messages
      LogService.setBaseLogLevel(Level.DEBUG);
      {
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, "0");
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + CLUSTER_SSL_ENABLED, "true");
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + CLUSTER_SSL_REQUIRE_AUTHENTICATION, "true");
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + CLUSTER_SSL_CIPHERS, "any");
        System.setProperty(DistributionConfig.GEMFIRE_PREFIX + CLUSTER_SSL_PROTOCOLS, "TLSv1.2");

        File jks = findTestJKS();
        System.setProperty("javax.net.ssl.trustStore", jks.getCanonicalPath());
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStore", jks.getCanonicalPath());
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
      }

      DistributionConfigImpl distributionConfig = new DistributionConfigImpl(new Properties());

      SocketCreatorFactory.setDistributionConfig(distributionConfig);
      assertTrue(SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).useSSL());

      final ServerSocket serverSocket = SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).createServerSocket(randport, 0, InetAddress.getByName("localhost"));

      Thread serverThread = startServer(serverSocket, receiver);

      Socket client = SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).connectForServer(InetAddress.getByName("localhost"), randport);

      ObjectOutputStream oos = new ObjectOutputStream(client.getOutputStream());
      String expected = "testing " + name.getMethodName();
      oos.writeObject(expected);
      oos.flush();

      ThreadUtils.join(serverThread, 30 * 1000);

      client.close();
      serverSocket.close();
      assertEquals("Expected \"" + expected + "\" but received \"" + receiver[0] + "\"", expected, receiver[0]);

      String stdOut = systemOutRule.getLog();
      int foundExpectedString = 0;

      Pattern pattern = Pattern.compile(".*peer CN=.*");
      Matcher matcher = pattern.matcher(stdOut);
      while (matcher.find()) {
        foundExpectedString++;
      }

      assertEquals(2, foundExpectedString);

    } finally {
      // Reset original base log level
      LogService.setBaseLogLevel(originalBaseLevel);
    }
  }

  /**
   * not actually related to this test class, but this is as good a place
   * as any for this little test of the client-side ability to tell gemfire
   * to use a given socket factory.  We just test the connectForClient method
   * to see if it's used
   */
  @Test
  public void testClientSocketFactory() {
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + "clientSocketFactory", TSocketFactory.class.getName());
    System.getProperties().put(DistributionConfig.GEMFIRE_PREFIX + CLUSTER_SSL_ENABLED, "false");
    DistributionConfigImpl distributionConfig = new DistributionConfigImpl(new Properties());
    SocketCreatorFactory.setDistributionConfig(distributionConfig);
    factoryInvoked = false;
    try {
      try {
        Socket sock = SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).connectForClient("localhost", 12345, 0);
        sock.close();
        fail("socket factory was invoked");
      } catch (IOException e) {
        assertTrue("socket factory was not invoked: " + factoryInvoked, factoryInvoked);
      }
    } finally {
      System.getProperties().remove(DistributionConfig.GEMFIRE_PREFIX + "clientSocketFactory");
      SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).initializeClientSocketFactory();
    }
  }

  private File findTestJKS() {
    return new File(TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore"));
  }

  private Thread startServer(final ServerSocket serverSocket, final Object[] receiver) throws Exception {
    Thread t = new Thread(new Runnable() {
      public void run() {
        try {
          Socket s = serverSocket.accept();
          SocketCreatorFactory.getSocketCreatorForComponent(SecurableCommunicationChannel.CLUSTER).configureServerSSLSocket(s);
          ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
          receiver[0] = ois.readObject();
          server = s;
          acceptor = serverSocket;
        } catch (Exception e) {
          e.printStackTrace();
          receiver[0] = e;
        }
      }
    }, name.getMethodName() + "-server");
    t.start();
    return t;
  }

  private static class TSocketFactory implements ClientSocketFactory {

    public TSocketFactory() {
    }

    public Socket createSocket(InetAddress address, int port) throws IOException {
      JSSESocketJUnitTest.factoryInvoked = true;
      throw new IOException("splort!");
    }
  }
}
