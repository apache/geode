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

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.geode.internal.net.SocketCreator.addServerNameIfNotSet;
import static org.apache.geode.test.util.ResourceUtils.createTempFileFromResource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
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

import org.apache.geode.distributed.internal.tcpserver.HostAndPort;
import org.apache.geode.net.SSLParameterExtension;

@Tag("membership")
class SocketCreatorTest {

  private final SSLContext context = mock(SSLContext.class);
  private final SSLParameters parameters = mock(SSLParameters.class);
  private final SSLEngine engine = mock(SSLEngine.class);

  SocketCreatorTest() {
    when(engine.getSSLParameters()).thenReturn(parameters);
  }


  @Test
  void testCreateSocketCreatorWithKeystoreUnset() {
    SSLConfig.Builder sslConfigBuilder = SSLConfig.builder();
    sslConfigBuilder.setEnabled(true);
    sslConfigBuilder.setKeystore(null);
    sslConfigBuilder.setKeystorePassword("");
    sslConfigBuilder.setTruststore(getSingleKeyKeystore());
    sslConfigBuilder.setTruststorePassword("password");
    // This would fail with java.io.FileNotFoundException: $USER_HOME/.keystore
    new SocketCreator(sslConfigBuilder.build());
  }

  @Test
  void testConfigureServerSSLSocketSetsSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final SSLSocket socket = mock(SSLSocket.class);

    final int timeout = 1938236;
    socketCreator.forCluster().handshakeIfSocketIsSSL(socket, timeout);
    verify(socket).setSoTimeout(timeout);
  }

  @Test
  void testConfigureServerPlainSocketDoesntSetSoTimeout() throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));
    final Socket socket = mock(Socket.class);
    final int timeout = 1938236;

    socketCreator.forCluster().handshakeIfSocketIsSSL(socket, timeout);
    verify(socket, never()).setSoTimeout(timeout);
  }

  @Test
  void configureSSLEngineSetsClientModeTrue() {
    final SSLConfig config = SSLConfig.builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLEngine(engine, "localhost", 12345, true);

    verify(engine).setUseClientMode(eq(true));
    verify(engine).getSSLParameters();
    verify(engine).setSSLParameters(eq(parameters));
    verifyNoMoreInteractions(parameters, engine);
  }

  @Test
  void configureSSLEngineSetsClientModeFalse() {
    final SSLConfig config = SSLConfig.builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLEngine(engine, "localhost", 12345, false);

    verify(engine).setUseClientMode(eq(false));
    verify(engine).getSSLParameters();
    verify(engine).setSSLParameters(eq(parameters));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters, engine);
  }

  @Test
  void configureSSLParametersSetsProtocolsWhenSetProtocolsAndWhenClientSocket() {
    final SSLConfig config = SSLConfig.builder().setProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsProtocolsWhenSetProtocolsAndServerSocket() {
    final SSLConfig config = SSLConfig.builder().setProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsProtocolsWhenSetClientProtocols() {
    final SSLConfig config =
        SSLConfig.builder().setClientProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsProtocolsWhenSetServerProtocols() {
    final SSLConfig config =
        SSLConfig.builder().setServerProtocols("protocol1,protocol2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setProtocols(eq(new String[] {"protocol1", "protocol2"}));
    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersDoesNotSetProtocolsWhenSetProtocolsIsAnyAndClientSocket() {
    final SSLConfig config = SSLConfig.builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersDoesNotSetProtocolsWhenSetProtocolsIsAnyAndServerSocket() {
    final SSLConfig config = SSLConfig.builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersDoesNotSetProtocolsWhenSetClientProtocolsIsAny() {
    final SSLConfig config = SSLConfig.builder().setClientProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersDoesNotSetProtocolsWhenSetServerProtocolsIsAny() {
    final SSLConfig config = SSLConfig.builder().setProtocols("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(anyBoolean());
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsCipherSuites() {
    final SSLConfig config = SSLConfig.builder().setCiphers("cipher1,cipher2").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setCipherSuites(eq(new String[] {"cipher1", "cipher2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersDoesNotSetCipherSuites() {
    final SSLConfig config = SSLConfig.builder().setCiphers("any").build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsNeedClientAuthTrue() {
    final SSLConfig config = SSLConfig.builder().setRequireAuth(true).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(eq(true));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsNeedClientAuthFalse() {
    final SSLConfig config = SSLConfig.builder().setRequireAuth(false).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, false);

    verify(parameters).setNeedClientAuth(eq(false));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParametersSetsEndpointIdentificationAlgorithmToHttpsAndServerNames() {
    final SSLConfig config = SSLConfig.builder().setEndpointIdentificationEnabled(true).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verify(parameters).setEndpointIdentificationAlgorithm(eq("HTTPS"));
    verify(parameters).getServerNames();
    verify(parameters)
        .setServerNames(argThat(a -> a.size() == 1 && a.contains(new SNIHostName("localhost"))));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureSSLParameterDoesNotSetEndpointIdentificationAlgorithm() {
    final SSLConfig config =
        SSLConfig.builder().setEndpointIdentificationEnabled(false).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureSSLParameters(parameters, "localhost", 123, true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void checkAndEnableHostnameValidationSetsEndpointIdentificationAlgorithmHttpsWhenEnabled() {
    final SSLConfig config =
        SSLConfig.builder().setEndpointIdentificationEnabled(true).build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.checkAndEnableHostnameValidation(parameters);

    verify(parameters).setEndpointIdentificationAlgorithm(eq("HTTPS"));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void checkAndEnableHostnameValidationDoesNotSetEndpointIdentificationAlgorithmHttpsAndHostnameValidationDisabledLogShownSetsWhenDisabled() {
    final SSLConfig config = SSLConfig.builder()
        .setEndpointIdentificationEnabled(false)
        .build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.checkAndEnableHostnameValidation(parameters);

    assertThat(socketCreator).hasFieldOrPropertyWithValue("hostnameValidationDisabledLogShown",
        true);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureProtocolsSetsClientProtocolsWhenClientSocketIsTrue() {
    final SSLConfig config = SSLConfig.builder()
        .setServerProtocols("server1,server2")
        .setClientProtocols("client1,client2")
        .build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureProtocols(true, parameters);

    verify(parameters).setProtocols(eq(new String[] {"client1", "client2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureProtocolsSetsClientProtocolsWhenClientSocketIsFalse() {
    final SSLConfig config = SSLConfig.builder()
        .setServerProtocols("server1,server2")
        .setClientProtocols("client1,client2")
        .build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureProtocols(false, parameters);

    verify(parameters).setProtocols(eq(new String[] {"server1", "server2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureProtocolsDoesNotSetClientProtocolsWhenClientSocketIsTrueAndSslConfigClientProtocolsIsUnset() {
    final SSLConfig config = SSLConfig.builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureProtocols(true, parameters);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureProtocolsDoesNotSetServerProtocolsWhenClientSocketIsTrueAndSslConfigClientProtocolsIsUnset() {
    final SSLConfig config = SSLConfig.builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureProtocols(false, parameters);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureCipherSuitesSetsCiphersWhenNotAny() {
    final SSLConfig config = SSLConfig.builder()
        .setCiphers("cipher1,cipher2")
        .build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureCipherSuites(parameters);

    verify(parameters).setCipherSuites(eq(new String[] {"cipher1", "cipher2"}));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureCipherSuitesDoesNotSetCiphersWhenAny() {
    final SSLConfig config = SSLConfig.builder()
        .setCiphers("any")
        .build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureCipherSuites(parameters);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void configureCipherSuitesDoesNotSetCiphersWhenDefault() {
    final SSLConfig config = SSLConfig.builder().build();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.configureCipherSuites(parameters);

    verifyNoMoreInteractions(parameters);
  }

  @Test
  void addServerNameIfNotSetAddsServerNameWhenNotSet() {
    final HostAndPort address = new HostAndPort("host1", 1234);

    when(parameters.getServerNames()).thenReturn(null);

    addServerNameIfNotSet(parameters, address);

    verify(parameters).getServerNames();
    verify(parameters).setServerNames(eq(singletonList(new SNIHostName(address.getHostName()))));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void addServerNameIfNotSetAddsServerNameWhenServerNamesIsEmpty() {
    final HostAndPort address = new HostAndPort("host1", 1234);

    when(parameters.getServerNames()).thenReturn(emptyList());

    addServerNameIfNotSet(parameters, address);

    verify(parameters).getServerNames();
    verify(parameters).setServerNames(eq(singletonList(new SNIHostName(address.getHostName()))));
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void addServerNameIfNotSetDoesNotAddServerNameWhenServerNamesSet() {
    final HostAndPort address = new HostAndPort("host1", 1234);

    when(parameters.getServerNames())
        .thenReturn(singletonList(new SNIHostName(address.getHostName())));

    addServerNameIfNotSet(parameters, address);

    verify(parameters).getServerNames();
    verifyNoMoreInteractions(parameters);
  }

  @Test
  void applySSLParameterExtensionsModifiesSSLClientSocketParametersWhenSSLParameterExtensionSet() {
    final SSLConfig config = mock(SSLConfig.class);
    final SSLParameterExtension sslParameterExtension = mock(SSLParameterExtension.class);
    when(config.getSSLParameterExtension()).thenReturn(sslParameterExtension);

    final SocketCreator socketCreator = new SocketCreator(config, context);

    socketCreator.applySSLParameterExtensions(parameters);

    verify(sslParameterExtension).modifySSLClientSocketParameters(same(parameters));
    verifyNoMoreInteractions(parameters);
  }

  private String getSingleKeyKeystore() {
    return createTempFileFromResource(getClass(), "/ssl/trusted.keystore").getAbsolutePath();
  }
}
