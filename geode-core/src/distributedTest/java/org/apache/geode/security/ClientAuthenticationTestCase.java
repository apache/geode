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
package org.apache.geode.security;

import static org.apache.geode.internal.AvailablePort.SOCKET;
import static org.apache.geode.internal.AvailablePort.getRandomAvailablePort;
import static org.apache.geode.security.ClientAuthenticationTestUtils.createCacheClient;
import static org.apache.geode.security.ClientAuthenticationTestUtils.createCacheServer;
import static org.apache.geode.security.ClientAuthenticationTestUtils.registerAllInterest;
import static org.apache.geode.security.SecurityTestUtils.AUTHFAIL_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.AUTHREQ_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.Employee;
import static org.apache.geode.security.SecurityTestUtils.NOFORCE_AUTHREQ_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.NO_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.OTHER_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.SECURITY_EXCEPTION;
import static org.apache.geode.security.SecurityTestUtils.closeCache;
import static org.apache.geode.security.SecurityTestUtils.createCacheClient;
import static org.apache.geode.security.SecurityTestUtils.doGets;
import static org.apache.geode.security.SecurityTestUtils.doLocalGets;
import static org.apache.geode.security.SecurityTestUtils.doNGets;
import static org.apache.geode.security.SecurityTestUtils.doNLocalGets;
import static org.apache.geode.security.SecurityTestUtils.doNPuts;
import static org.apache.geode.security.SecurityTestUtils.doProxyCacheClose;
import static org.apache.geode.security.SecurityTestUtils.doPuts;
import static org.apache.geode.security.SecurityTestUtils.doSimpleGet;
import static org.apache.geode.security.SecurityTestUtils.doSimplePut;
import static org.apache.geode.security.SecurityTestUtils.registerExpectedExceptions;
import static org.apache.geode.security.SecurityTestUtils.verifyIsEmptyOnServer;
import static org.apache.geode.security.SecurityTestUtils.verifySizeOnServer;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.LogWriterUtils.getLogWriter;
import static org.apache.geode.test.dunit.Wait.pause;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Properties;

import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;

import org.junit.Assert;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.client.Pool;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.ServerOperationException;
import org.apache.geode.cache.client.internal.ExecutablePool;
import org.apache.geode.cache.client.internal.RegisterDataSerializersOp;
import org.apache.geode.internal.HeapDataOutputStream;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.sockets.ServerConnection;
import org.apache.geode.security.generator.CredentialGenerator;
import org.apache.geode.security.generator.DummyCredentialGenerator;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.dunit.standalone.VersionManager;

public abstract class ClientAuthenticationTestCase extends JUnit4DistributedTestCase {

  private VM server1 = null;
  private VM server2 = null;
  private VM client1 = null;
  private VM client2 = null;

  private static final String[] serverIgnoredExceptions =
      {AuthenticationRequiredException.class.getName(),
          AuthenticationFailedException.class.getName(), GemFireSecurityException.class.getName(),
          ClassNotFoundException.class.getName(), IOException.class.getName(),
          SSLException.class.getName(), SSLHandshakeException.class.getName()};

  private static final String[] clientIgnoredExceptions =
      {AuthenticationRequiredException.class.getName(),
          AuthenticationFailedException.class.getName(), SSLHandshakeException.class.getName()};


  public static enum Color {
    red, orange, yellow, green, blue, indigo, violet
  }


  public static class MyDataSerializer extends DataSerializer {
    public MyDataSerializer() {}

    @Override
    public Class<?>[] getSupportedClasses() {
      return new Class[] {Color.class};
    }

    public int getId() {
      return 1073741824;
    }

    @Override
    public boolean toData(Object object, DataOutput output) {
      return true;
    }

    @Override
    public Object fromData(DataInput in) throws IOException, ClassNotFoundException {
      return Color.red;
    }
  }

  public String clientVersion = VersionManager.CURRENT_VERSION;

  @Override
  public final void postSetUp() throws Exception {
    final Host host = Host.getHost(0);
    server1 = host.getVM(VersionManager.CURRENT_VERSION, 0);
    server2 = host.getVM(VersionManager.CURRENT_VERSION, 1);
    server1.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = false);
    server2.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = false);
    if (VersionManager.isCurrentVersion(clientVersion)) {
      client1 = host.getVM(VersionManager.CURRENT_VERSION, 2);
      client2 = host.getVM(VersionManager.CURRENT_VERSION, 3);
    } else {
      client1 = host.getVM(clientVersion, 2);
      client2 = host.getVM(clientVersion, 3);
    }

    addIgnoredException("Connection refused: connect");

    server1.invoke(() -> registerExpectedExceptions(serverIgnoredExceptions));
    server2.invoke(() -> registerExpectedExceptions(serverIgnoredExceptions));
    client1.invoke(() -> registerExpectedExceptions(clientIgnoredExceptions));
    client2.invoke(() -> registerExpectedExceptions(clientIgnoredExceptions));
  }

  @Override
  public void postTearDown() throws Exception {
    super.postTearDown();
    server1.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = true);
    server2.invoke(() -> ServerConnection.allowInternalMessagesWithoutCredentials = true);
  }


  protected void doTestValidCredentials(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testValidCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testValidCredentials: Using authenticator: " + authenticator);
    getLogWriter().info("testValidCredentials: Using authinit: " + authInit);

    // Start the servers
    int port1 = server1
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();

    getLogWriter().info(
        "testValidCredentials: For first client credentials: " + credentials1 + " : " + javaProps1);

    Properties credentials2 = gen.getValidCredentials(2);
    Properties javaProps2 = gen.getJavaProperties();

    getLogWriter().info("testValidCredentials: For second client credentials: " + credentials2
        + " : " + javaProps2);

    createClientsNoException(multiUser, authInit, port1, port2, credentials1, javaProps1,
        credentials2, javaProps2);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));

    // Verify that the puts succeeded
    client2.invoke(() -> doGets(2));

    // Verify sizeOnServer is correct
    client1.invoke(() -> verifySizeOnServer(2));
    client1.invoke(() -> verifyIsEmptyOnServer(false));
    client2.invoke(() -> verifySizeOnServer(2));
    client2.invoke(() -> verifyIsEmptyOnServer(false));

    if (multiUser) {
      client1.invoke(() -> doProxyCacheClose());
      client2.invoke(() -> doProxyCacheClose());
      client1.invoke(() -> doSimplePut("CacheClosedException"));
      client2.invoke(() -> doSimpleGet("CacheClosedException"));
    }
  }

  protected void doTestNoCredentials(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testNoCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testNoCredentials: Using authenticator: " + authenticator);
    getLogWriter().info("testNoCredentials: Using authinit: " + authInit);

    // Start the servers
    int port1 = createServer1(extraProps, javaProps, authenticator);
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start first client with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();

    getLogWriter().info(
        "testNoCredentials: For first client credentials: " + credentials1 + " : " + javaProps1);

    createClient1NoException(multiUser, authInit, port1, port2, credentials1, javaProps1);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));

    // Trying to create the region on client2
    if (gen.classCode().equals(CredentialGenerator.ClassCode.SSL)) {
      // For SSL the exception may not come since the server can close socket
      // before handshake message is sent from client. However exception
      // should come in any region operations.
      client2.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, NO_EXCEPTION));
      client2.invoke(() -> doPuts(2, OTHER_EXCEPTION));

    } else {
      client2.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, AUTHREQ_EXCEPTION));
    }
  }

  protected void doTestNoCredentialsCantRegisterMetadata(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    // Start the servers
    int port1 = createServer1(extraProps, javaProps, authenticator);
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start first client with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();

    createClient1NoException(multiUser, authInit, port1, port2, credentials1, javaProps1);

    // Trying to create the region on client2
    if (gen.classCode().equals(CredentialGenerator.ClassCode.SSL)) {
      // For SSL the exception may not come since the server can close socket
      // before handshake message is sent from client. However exception
      // should come in any region operations.
      client2.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, NO_EXCEPTION));
      client2.invoke(() -> doPuts(2, OTHER_EXCEPTION));

    } else {
      client1.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, AUTHREQ_EXCEPTION));

      // Try to register a PDX type with the server
      client1.invoke("register a PDX type", () -> {
        HeapDataOutputStream outputStream = new HeapDataOutputStream(100, Version.CURRENT);
        try {
          DataSerializer.writeObject(new Employee(106l, "David", "Copperfield"), outputStream);
          throw new Error("operation should have been rejected");
        } catch (ServerOperationException e) {
          Assert.assertEquals(AuthenticationRequiredException.class, e.getCause().getClass());
        } catch (UnsupportedOperationException e) {
          // expected
        }
      });

      client2.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, AUTHREQ_EXCEPTION));

      // Try to register a DataSerializer with the server
      client2.invoke("register a data serializer", () -> {
        EventID eventId = InternalDataSerializer.generateEventId();
        Pool pool = PoolManager.getAll().values().iterator().next();
        try {
          RegisterDataSerializersOp.execute((ExecutablePool) pool,
              new DataSerializer[] {new MyDataSerializer()}, eventId);
          throw new Error("operation should have been rejected");
        } catch (ServerOperationException e) {
          Assert.assertEquals(AuthenticationRequiredException.class, e.getCause().getClass());
        } catch (UnsupportedOperationException e) {
          // expected
        }
      });
    }

  }

  protected void doTestInvalidCredentials(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testInvalidCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testInvalidCredentials: Using authenticator: " + authenticator);
    getLogWriter().info("testInvalidCredentials: Using authinit: " + authInit);

    // Start the servers
    int port1 = createServer1(extraProps, javaProps, authenticator);
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start first client with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info("testInvalidCredentials: For first client credentials: " + credentials1
        + " : " + javaProps1);

    createClient1NoException(multiUser, authInit, port1, port2, credentials1, javaProps1);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));
    client1.invoke(() -> verifySizeOnServer(2));
    client1.invoke(() -> verifyIsEmptyOnServer(false));

    // Start second client with invalid credentials
    // Trying to create the region on client2 should throw a security
    // exception
    Properties credentials2 = gen.getInvalidCredentials(1);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testInvalidCredentials: For second client credentials: " + credentials2
        + " : " + javaProps2);

    client2.invoke(() -> createCacheClient(authInit, credentials2, javaProps2, port1, port2, 0,
        multiUser, AUTHFAIL_EXCEPTION));
  }

  protected void doTestInvalidAuthInit(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    final Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();

    getLogWriter().info("testInvalidAuthInit: Using scheme: " + gen.classCode());
    getLogWriter().info("testInvalidAuthInit: Using authenticator: " + authenticator);

    // Start the server
    int port1 = createServer1(extraProps, javaProps, authenticator);
    Properties credentials = gen.getValidCredentials(1);
    getLogWriter().info(
        "testInvalidAuthInit: For first client credentials: " + credentials + " : " + javaProps);

    client1.invoke(() -> createCacheClient("org.apache.none", credentials, javaProps,
        new int[] {port1}, 0, false, multiUser, true, SECURITY_EXCEPTION));
  }

  protected void doTestNoAuthInitWithCredentials(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();

    getLogWriter().info("testNoAuthInitWithCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testNoAuthInitWithCredentials: Using authenticator: " + authenticator);

    // Start the servers
    int port1 = createServer1(extraProps, javaProps, authenticator);
    int port2 = server2
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(1);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info("testNoAuthInitWithCredentials: For first client credentials: "
        + credentials1 + " : " + javaProps1);

    Properties credentials2 = gen.getValidCredentials(2);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testNoAuthInitWithCredentials: For second client credentials: "
        + credentials2 + " : " + javaProps2);

    client1.invoke(() -> createCacheClient(null, credentials1, javaProps1, port1, port2, 0,
        multiUser, AUTHREQ_EXCEPTION));
    client2.invoke(() -> createCacheClient(null, credentials2, javaProps2, port1, port2, 0,
        multiUser, AUTHREQ_EXCEPTION));
    client2.invoke(() -> closeCache());

    // Now also try with invalid credentials
    Properties credentials3 = gen.getInvalidCredentials(5);
    Properties javaProps3 = gen.getJavaProperties();

    client2.invoke(() -> createCacheClient(null, credentials3, javaProps3, port1, port2, 0,
        multiUser, AUTHREQ_EXCEPTION));
  }

  /**
   * NOTE: "final boolean multiUser" is unused
   */
  protected void doTestInvalidAuthenticator(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testInvalidAuthenticator: Using scheme: " + gen.classCode());
    getLogWriter().info("testInvalidAuthenticator: Using authinit: " + authInit);

    // Start the server with invalid authenticator
    server1.invoke(() -> createCacheServer("org.apache.geode.none", extraProps,
        javaProps, AUTHREQ_EXCEPTION));
  }

  protected void doTestNoAuthenticatorWithCredentials(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testNoAuthenticatorWithCredentials: Using scheme: " + gen.classCode());
    getLogWriter().info("testNoAuthenticatorWithCredentials: Using authinit: " + authInit);

    // Start the servers with no authenticator
    int port1 =
        server1.invoke(() -> createCacheServer(null, extraProps, javaProps));
    int port2 =
        server2.invoke(() -> createCacheServer(null, extraProps, javaProps));

    // Clients should connect successfully and work properly with
    // valid/invalid credentials when none are required on the server side
    Properties credentials1 = gen.getValidCredentials(3);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info("testNoAuthenticatorWithCredentials: For first client credentials: "
        + credentials1 + " : " + javaProps1);

    Properties credentials2 = gen.getInvalidCredentials(5);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testNoAuthenticatorWithCredentials: For second client credentials: "
        + credentials2 + " : " + javaProps2);

    createClientsNoException(multiUser, authInit, port1, port2, credentials1, javaProps1,
        credentials2, javaProps2);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));

    // Verify that the puts succeeded
    client2.invoke(() -> doGets(2));
  }

  protected void doTestCredentialsWithFailover(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testCredentialsWithFailover: Using scheme: " + gen.classCode());
    getLogWriter().info("testCredentialsWithFailover: Using authenticator: " + authenticator);
    getLogWriter().info("testCredentialsWithFailover: Using authinit: " + authInit);

    // Start the first server
    int port1 = server1
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Get a port for second server but do not start it
    // This forces the clients to connect to the first server
    int port2 = getRandomAvailablePort(SOCKET);

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(5);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsWithFailover: For first client credentials: " + credentials1
        + " : " + javaProps1);

    Properties credentials2 = gen.getValidCredentials(6);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsWithFailover: For second client credentials: "
        + credentials2 + " : " + javaProps2);

    createClientsNoException(multiUser, authInit, port1, port2, credentials1, javaProps1,
        credentials2, javaProps2);

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));
    // Verify that the puts succeeded
    client2.invoke(() -> doGets(2));

    // start the second one and stop the first server to force a failover
    server2.invoke(
        () -> createCacheServer(port2, authenticator, extraProps, javaProps));
    server1.invoke(() -> closeCache());

    // Perform some create/update operations from client1
    client1.invoke(() -> doNPuts(4));
    // Verify that the creates/updates succeeded
    client2.invoke(() -> doNGets(4));

    // Try to connect client2 with no credentials
    // Verify that the creation of region throws security exception
    if (gen.classCode().equals(CredentialGenerator.ClassCode.SSL)) {
      // For SSL the exception may not come since the server can close socket
      // before handshake message is sent from client. However exception
      // should come in any region operations.
      client2.invoke(() -> createCacheClient(null, null, null, port1, port2, 0, multiUser,
          NOFORCE_AUTHREQ_EXCEPTION));
      client2.invoke(() -> doPuts(2, OTHER_EXCEPTION));

    } else {
      client2.invoke(
          () -> createCacheClient(null, null, null, port1, port2, 0, multiUser, AUTHREQ_EXCEPTION));
    }

    // Now try to connect client1 with invalid credentials
    // Verify that the creation of region throws security exception
    Properties credentials3 = gen.getInvalidCredentials(7);
    Properties javaProps3 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsWithFailover: For first client invalid credentials: "
        + credentials3 + " : " + javaProps3);

    client1.invoke(() -> createCacheClient(authInit, credentials3, javaProps3, port1, port2, 0,
        multiUser, AUTHFAIL_EXCEPTION));

    if (multiUser) {
      client1.invoke(() -> doProxyCacheClose());
      client2.invoke(() -> doProxyCacheClose());
      client1.invoke(() -> doSimplePut("CacheClosedException"));
      client2.invoke(() -> doSimpleGet("CacheClosedException"));
    }
  }

  protected void doTestCredentialsForNotifications(final boolean multiUser) throws Exception {
    CredentialGenerator gen = new DummyCredentialGenerator();
    Properties extraProps = gen.getSystemProperties();
    Properties javaProps = gen.getJavaProperties();
    String authenticator = gen.getAuthenticator();
    String authInit = gen.getAuthInit();

    getLogWriter().info("testCredentialsForNotifications: Using scheme: " + gen.classCode());
    getLogWriter().info("testCredentialsForNotifications: Using authenticator: " + authenticator);
    getLogWriter().info("testCredentialsForNotifications: Using authinit: " + authInit);

    // Start the first server
    int port1 = server1
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));

    // Get a port for second server but do not start it
    // This forces the clients to connect to the first server
    int port2 = getRandomAvailablePort(SOCKET);

    // Start the clients with valid credentials
    Properties credentials1 = gen.getValidCredentials(3);
    Properties javaProps1 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsForNotifications: For first client credentials: "
        + credentials1 + " : " + javaProps1);

    Properties credentials2 = gen.getValidCredentials(4);
    Properties javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsForNotifications: For second client credentials: "
        + credentials2 + " : " + javaProps2);

    createClient1NoException(multiUser, authInit, port1, port2, credentials1, javaProps1);

    // Set up zero forward connections to check notification handshake only
    int zeroConns = 0;
    createClient2NoException(multiUser, authInit, port1, port2, credentials2, javaProps2,
        zeroConns);

    // Register interest on all keys on second client
    client2.invoke(() -> registerAllInterest());

    // Perform some put operations from client1
    client1.invoke(() -> doPuts(2));

    // Verify that the puts succeeded
    client2.invoke(() -> doLocalGets(2));

    // start the second one and stop the first server to force a failover
    server2.invoke(
        () -> createCacheServer(port2, authenticator, extraProps, javaProps));
    server1.invoke(() -> closeCache());

    // Wait for failover to complete
    pause(500);

    // Perform some create/update operations from client1
    client1.invoke(() -> doNPuts(4));
    // Verify that the creates/updates succeeded
    client2.invoke(() -> doNLocalGets(4));

    // Try to connect client1 with no credentials
    // Verify that the creation of region throws security exception
    final int p = server1.invoke(
        () -> createCacheServer(0, authenticator, extraProps, javaProps));
    if (gen.classCode().equals(CredentialGenerator.ClassCode.SSL)) {
      // For SSL the exception may not come since the server can close socket
      // before handshake message is sent from client. However exception
      // should come in any region operations.
      client1.invoke(() -> createCacheClient(null, null, null, p, port2, zeroConns, multiUser,
          NOFORCE_AUTHREQ_EXCEPTION));
      client1.invoke(() -> doPuts(2, OTHER_EXCEPTION));

    } else {
      client1.invoke(() -> createCacheClient(null, null, null, p, port2, zeroConns, multiUser,
          AUTHREQ_EXCEPTION));
    }

    // Now try to connect client2 with invalid credentials
    // Verify that the creation of region throws security exception
    credentials2 = gen.getInvalidCredentials(3);
    javaProps2 = gen.getJavaProperties();
    getLogWriter().info("testCredentialsForNotifications: For second client invalid credentials: "
        + credentials2 + " : " + javaProps2);

    createClient2WithException(multiUser, authInit, p, port2, credentials2, javaProps2, zeroConns);

    // Now try to connect client2 with invalid auth-init method
    // Trying to create the region on client with valid credentials should
    // throw a security exception
    client2.invoke(() -> createCacheClient("org.apache.none", credentials1, javaProps1, p, port2,
        zeroConns, multiUser, SECURITY_EXCEPTION));

    // Try connection with null auth-init on clients.
    // Skip this test for a scheme which does not have an authInit in the
    // first place (e.g. SSL).
    if (authInit != null && authInit.length() > 0) {
      final int p1 = server1.invoke(
          () -> createCacheServer(0, authenticator, extraProps, javaProps));
      final int p2 = server2.invoke(
          () -> createCacheServer(0, authenticator, extraProps, javaProps));
      client1.invoke(() -> createCacheClient(null, credentials1, javaProps1, p1, p2, 0, multiUser,
          AUTHREQ_EXCEPTION));

      createClient2AuthReqException(multiUser, p1, p2, credentials2, javaProps2, zeroConns);
      createClient2AuthReqException(multiUser, p1, p2, credentials2, javaProps2, zeroConns);

    } else {
      getLogWriter().info("testCredentialsForNotifications: Skipping null authInit for scheme ["
          + gen.classCode() + "] which has no authInit");
    }

    // Try connection with null authenticator on server and sending
    // valid/invalid credentials.
    // If the scheme does not have an authenticator in the first place (e.g.
    // SSL) then skip it since this test is useless.
    if (authenticator != null && authenticator.length() > 0) {
      final int p1 = server1
          .invoke(() -> createCacheServer(0, null, extraProps, javaProps));
      final int p2 = server2
          .invoke(() -> createCacheServer(0, null, extraProps, javaProps));

      createClient1NoException(multiUser, authInit, p1, p2, credentials1, javaProps1);
      createClient2NoException(multiUser, authInit, p1, p2, credentials2, javaProps2, zeroConns);

      // Register interest on all keys on second client
      client2.invoke(() -> registerAllInterest());

      // Perform some put operations from client1
      client1.invoke(() -> doPuts(4));

      // Verify that the puts succeeded
      client2.invoke(() -> doLocalGets(4));

      // Now also try with valid credentials on client2
      createClient1NoException(multiUser, authInit, p1, p2, credentials2, javaProps2);
      createClient2NoException(multiUser, authInit, p1, p2, credentials1, javaProps1, zeroConns);

      // Register interest on all keys on second client
      client2.invoke(() -> registerAllInterest());

      // Perform some put operations from client1
      client1.invoke(() -> doNPuts(4));

      // Verify that the puts succeeded
      client2.invoke(() -> doNLocalGets(4));

    } else {
      getLogWriter().info("testCredentialsForNotifications: Skipping scheme [" + gen.classCode()
          + "] which has no authenticator");
    }
  }

  private int createServer1(final Properties extraProps, final Properties javaProps,
      final String authenticator) {
    return server1
        .invoke(() -> createCacheServer(authenticator, extraProps, javaProps));
  }

  private void createClient1NoException(final boolean multiUser, final String authInit,
      final int port1, final int port2, final Properties credentials2,
      final Properties javaProps2) {
    client1.invoke(() -> createCacheClient(authInit, credentials2, javaProps2, port1, port2, 0,
        multiUser, NO_EXCEPTION));
  }

  private void createClient2AuthReqException(final boolean multiUser, final int port1,
      final int port2, final Properties credentials2, final Properties javaProps2,
      final int zeroConns) {
    client2.invoke(() -> createCacheClient(null, credentials2, javaProps2, port1, port2, zeroConns,
        multiUser, AUTHREQ_EXCEPTION));
  }

  private void createClient2WithException(final boolean multiUser, final String authInit,
      final int port1, final int port2, final Properties credentials2, final Properties javaProps2,
      final int zeroConns) {
    client2.invoke(() -> createCacheClient(authInit, credentials2, javaProps2, port1, port2,
        zeroConns, multiUser, AUTHFAIL_EXCEPTION));
  }

  private void createClient2NoException(final boolean multiUser, final String authInit,
      final int port1, final int port2, final Properties credentials2, final Properties javaProps2,
      final int zeroConns) {
    client2.invoke(() -> createCacheClient(authInit, credentials2, javaProps2, port1, port2,
        zeroConns, multiUser, NO_EXCEPTION));
  }

  private void createClientsNoException(final boolean multiUser, final String authInit,
      final int port1, final int port2, final Properties credentials1, final Properties javaProps1,
      final Properties credentials2, final Properties javaProps2) {
    createClient1NoException(multiUser, authInit, port1, port2, credentials1, javaProps1);
    client2.invoke(() -> createCacheClient(authInit, credentials2, javaProps2, port1, port2, 0,
        multiUser, NO_EXCEPTION));
  }
}
