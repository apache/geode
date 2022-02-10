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
package org.apache.geode.internal.net;

import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.net.Socket;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("membership")
public class SocketCreatorTest {

  private final SSLContext context = mock(SSLContext.class);
  private final SSLParameters parameters = mock(SSLParameters.class);
  private final SSLEngine engine = mock(SSLEngine.class);

  public SocketCreatorTest() {
    when(engine.getSSLParameters()).thenReturn(parameters);
  }


  @Test
  public void testCreateSocketCreatorWithKeystoreUnset() {
    SSLConfig.Builder sslConfigBuilder = new SSLConfig.Builder();
    sslConfigBuilder.setEnabled(true);
    sslConfigBuilder.setKeystore(null);
    sslConfigBuilder.setKeystorePassword("");
    sslConfigBuilder.setTruststore(getSingleKeyKeystore());
    sslConfigBuilder.setTruststorePassword("password");
    // This would fail with java.io.FileNotFoundException: $USER_HOME/.keystore
    new SocketCreator(sslConfigBuilder.build());
  }

  @Test
  public void testConfigureServerSSLSocketSetsSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final SSLSocket socket = mock(SSLSocket.class);

    final int timeout = 1938236;
    socketCreator.forCluster().handshakeIfSocketIsSSL(socket, timeout);
    verify(socket).setSoTimeout(timeout);
  }

  @Test
  public void testConfigureServerPlainSocketDoesntSetSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final Socket socket = mock(Socket.class);
    final int timeout = 1938236;

    socketCreator.forCluster().handshakeIfSocketIsSSL(socket, timeout);
    verify(socket, never()).setSoTimeout(timeout);
  }

  @Test
  public void configureSSLEngineSetsClientModeTrue() {
    final SSLConfig config = new SSLConfig.Builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLEngine(engine, "localhost", 12345, true);

    verify(engine).setUseClientMode(eq(true));
    verify(engine).getSSLParameters();
    verify(engine).setSSLParameters(eq(parameters));
    verifyNoMoreInteractions(parameters, engine);
  }

  @Test
  public void configureSSLEngineSetsClientModeFalse() {
    final SSLConfig config = new SSLConfig.Builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLEngine(engine, "localhost", 12345, false);

    verify(engine).setUseClientMode(eq(false));
    verify(engine).getSSLParameters();
    verify(engine).setSSLParameters(eq(parameters));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters, engine);
  }

  @Test
  public void configureSSLParametersSetsProtocolsWhenSetProtocolsAndWhenClientSocket() {
    final SSLConfig config = new SSLConfig.Builder().setProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsProtocolsWhenSetProtocolsAndServerSocket() {
    final SSLConfig config = new SSLConfig.Builder().setProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsProtocolsWhenSetClientProtocols() {
    final SSLConfig config =
        new SSLConfig.Builder().setClientProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsProtocolsWhenSetServerProtocols() {
    final SSLConfig config =
        new SSLConfig.Builder().setServerProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersDoesNotSetProtocolsWhenSetProtocolsIsAnyAndClientSocket() {
    final SSLConfig config = new SSLConfig.Builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersDoesNotSetProtocolsWhenSetProtocolsIsAnyAndServerSocket() {
    final SSLConfig config = new SSLConfig.Builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersDoesNotSetProtocolsWhenSetClientProtocolsIsAny() {
    final SSLConfig config = new SSLConfig.Builder().setClientProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersDoesNotSetProtocolsWhenSetServerProtocolsIsAny() {
    final SSLConfig config = new SSLConfig.Builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsCipherSuites() {
    final SSLConfig config = new SSLConfig.Builder().setCiphers("cipher1,cipher2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setCipherSuites(eq(new String[] {"cipher1", "cipher2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersDoesNotSetCipherSuites() {
    final SSLConfig config = new SSLConfig.Builder().setCiphers("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsNeedClientAuthTrue() {
    final SSLConfig config = new SSLConfig.Builder().setRequireAuth(true).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(eq(true));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsNeedClientAuthFalse() {
    final SSLConfig config = new SSLConfig.Builder().setRequireAuth(false).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(eq(false));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParametersSetsEndpointIdentificationAlgorithmToHttpsAndServerNames() {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setEndpointIdentificationAlgorithm(eq("HTTPS"));
    verify(parameters).getServerNames();
    verify(parameters)
        .setServerNames(argThat(a -> a.size() == 1 && a.contains(new SNIHostName("localhost"))));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  public void configureSSLParameterDoesNotSetEndpointIdentificationAlgorithm() {
    final SSLConfig config =
        new SSLConfig.Builder().setEndpointIdentificationEnabled(false).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  private String getSingleKeyKeystore() {
    return createTempFileFromResource(getClass(), "/ssl/trusted.keystore").getAbsolutePath();
  }
}
