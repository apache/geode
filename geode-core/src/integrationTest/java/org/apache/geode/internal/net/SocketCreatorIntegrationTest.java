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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.junit.jupiter.api.Test;

import org.apache.geode.internal.AvailablePortHelper;

public class SocketCreatorIntegrationTest {

  @Test
  public void testBindExceptionMessageFormattingWithBindAddress() throws Exception {
    testBindExceptionMessageFormatting(InetAddress.getLocalHost());
  }

  @Test
  public void testBindExceptionMessageFormattingNullBindAddress() throws Exception {
    testBindExceptionMessageFormatting(null);
  }

  private void testBindExceptionMessageFormatting(InetAddress inetAddress) throws Exception {
    final SocketCreator socketCreator = new SocketCreator(mock(SSLConfig.class));

    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    try (ServerSocket ignored = socketCreator.forCluster()
        .createServerSocket(port, 10, inetAddress)) {
      assertThatExceptionOfType(BindException.class).isThrownBy(() -> {
        // call twice on the same port to trigger exception
        socketCreator.forCluster().createServerSocket(port, 10, inetAddress);
      }).withMessageContaining(Integer.toString(port))
          .withMessageContaining(InetAddress.getLocalHost().getHostAddress());
    }
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithAnyProtocolForClientWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true).build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine expectedEngine = context.createSSLEngine("localhost", 1234);
    expectedEngine.setUseClientMode(true);
    final String[] enabledProtocols = expectedEngine.getEnabledProtocols();

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, true);

    assertThat(engine.getEnabledProtocols()).containsExactly(enabledProtocols);
    assertThat(engine.getNeedClientAuth()).isFalse();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
    assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithSpecificProtocolForClientWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true)
        .setProtocols("TLSv1.2").build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, true);

    assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
    assertThat(engine.getNeedClientAuth()).isFalse();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
    assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithSpecificProtocolsForClientWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true)
        .setProtocols("TLSv1.2,SSLv2Hello").build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, true);

    assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2", "SSLv2Hello");
    assertThat(engine.getNeedClientAuth()).isFalse();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
    assertThat(engine.getSSLParameters().getEndpointIdentificationAlgorithm()).isEqualTo("HTTPS");
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithAnyProtocolForServerWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true).build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine expectedEngine = context.createSSLEngine("localhost", 1234);
    expectedEngine.setUseClientMode(false);
    final String[] enabledProtocols = expectedEngine.getEnabledProtocols();

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, false);

    assertThat(engine.getEnabledProtocols()).containsExactly(enabledProtocols);
    assertThat(engine.getNeedClientAuth()).isTrue();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithSpecificProtocolForServerWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true)
        .setProtocols("TLSv1.2").build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, false);

    assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2");
    assertThat(engine.getNeedClientAuth()).isTrue();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
  }

  @Test
  public void createSSLEngineReturnsConfiguredWithSpecificProtocolsForServerWhenEndpointIdentificationEnabled()
      throws NoSuchAlgorithmException {
    final SSLConfig config = new SSLConfig.Builder().setEndpointIdentificationEnabled(true)
        .setProtocols("TLSv1.2,SSLv2Hello").build();
    final SSLContext context = SSLContext.getDefault();
    final SocketCreator socketCreator = new SocketCreator(config, context);

    final SSLEngine engine = socketCreator.createSSLEngine("localhost", 1234, false);

    assertThat(engine.getEnabledProtocols()).containsExactly("TLSv1.2", "SSLv2Hello");
    assertThat(engine.getNeedClientAuth()).isTrue();
    assertThat(engine.getSSLParameters().getServerNames())
        .containsExactly(new SNIHostName("localhost"));
  }
}
