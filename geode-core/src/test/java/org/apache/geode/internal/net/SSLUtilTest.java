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

import static org.apache.geode.internal.net.SSLUtil.combineProtocols;
import static org.apache.geode.internal.net.SSLUtil.split;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import org.junit.jupiter.api.Test;


public class SSLUtilTest {

  @Test
  public void failWhenNothingIsRequested() {
    SSLConfig sslConfig = mock(SSLConfig.class);
    when(sslConfig.getClientProtocolsAsStringArray())
        .thenReturn(new String[0]);
    when(sslConfig.getServerProtocolsAsStringArray())
        .thenReturn(new String[0]);

    assertThatThrownBy(() -> SSLUtil.getSSLContextInstance(sslConfig))
        .isInstanceOf(NoSuchAlgorithmException.class);
  }

  @Test
  public void failWithAnUnknownProtocol() throws Exception {
    SSLConfig sslConfig = mock(SSLConfig.class);
    when(sslConfig.getClientProtocolsAsStringArray())
        .thenReturn(new String[] {"boulevard of broken dreams"});
    when(sslConfig.getServerProtocolsAsStringArray())
        .thenReturn(new String[] {"boulevard of broken dreams"});
    assertThatThrownBy(() -> SSLUtil.getSSLContextInstance(sslConfig))
        .isInstanceOf(NoSuchAlgorithmException.class);
  }

  @Test
  public void getASpecificProtocol() throws Exception {
    SSLConfig sslConfig = mock(SSLConfig.class);
    when(sslConfig.getClientProtocolsAsStringArray()).thenReturn(new String[] {"TLSv1.2"});
    when(sslConfig.getServerProtocolsAsStringArray()).thenReturn(new String[] {"TLSv1.2"});
    final SSLContext sslContextInstance = SSLUtil.getSSLContextInstance(sslConfig);
    assertThat(sslContextInstance.getProtocol().equalsIgnoreCase("TLSv1.2")).isTrue();
  }

  @Test
  public void getAnyProtocolWithAnUnknownInTheList() throws Exception {
    SSLConfig sslConfig = mock(SSLConfig.class);
    when(sslConfig.getClientProtocolsAsStringArray())
        .thenReturn(new String[] {"the dream of the blue turtles", "any", "SSL"});
    when(sslConfig.getServerProtocolsAsStringArray())
        .thenReturn(new String[] {"the dream of the blue turtles", "any", "SSL"});
    final SSLContext sslContextInstance = SSLUtil.getSSLContextInstance(sslConfig);
    // make sure that we don't continue past "any" and use the following protocol (SSL)
    assertThat(sslContextInstance.getProtocol().equalsIgnoreCase("SSL")).isFalse();
    String selectedProtocol = sslContextInstance.getProtocol();
    String matchedProtocol = null;
    for (String algorithm : SSLUtil.DEFAULT_ALGORITHMS) {
      if (algorithm.equalsIgnoreCase(selectedProtocol)) {
        matchedProtocol = algorithm;
      }
    }
    assertThat(matchedProtocol).withFailMessage(
        "selected protocol (%s) is not in the list of default algorithms, indicating that the \"any\" setting did not work correctly",
        selectedProtocol).isNotNull();
  }

  @Test
  public void getARealProtocolAfterProcessingAny() throws Exception {
    final String[] algorithms = {"dream weaver", "any", "TLSv1.2"};
    final String[] algorithmsForAny = new String[] {"sweet dreams (are made of this)"};
    final SSLContext sslContextInstance = SSLUtil.findSSLContextForProtocols(algorithms,
        algorithmsForAny);
    assertThat(sslContextInstance.getProtocol().equalsIgnoreCase("TLSv1.2")).isTrue();
  }

  @Test
  public void getDefaultKeyManagerFactory() throws NoSuchAlgorithmException {
    final KeyManagerFactory keyManagerFactory = SSLUtil.getDefaultKeyManagerFactory();
    assertThat(keyManagerFactory).isNotNull();
    assertThat(keyManagerFactory.getAlgorithm()).isEqualTo(KeyManagerFactory.getDefaultAlgorithm());
  }

  @Test
  public void getDefaultTrustManagerFactory() throws NoSuchAlgorithmException {
    final TrustManagerFactory trustManagerFactory = SSLUtil.getDefaultTrustManagerFactory();
    assertThat(trustManagerFactory).isNotNull();
    assertThat(trustManagerFactory.getAlgorithm())
        .isEqualTo(TrustManagerFactory.getDefaultAlgorithm());
  }

  @Test
  void combineProtocolsReturnsEmptyWhenBothProtocolListsAreEmpty() {
    final SSLConfig config = mock(SSLConfig.class);
    when(config.getClientProtocolsAsStringArray()).thenReturn(new String[0]);
    when(config.getServerProtocolsAsStringArray()).thenReturn(new String[0]);

    assertThat(combineProtocols(config)).isEmpty();
  }

  @Test
  void combineProtocolsReturnsUniqueValues() {
    final SSLConfig config = mock(SSLConfig.class);
    when(config.getClientProtocolsAsStringArray()).thenReturn(new String[] {"a"});
    when(config.getServerProtocolsAsStringArray()).thenReturn(new String[] {"a", "b"});

    assertThat(combineProtocols(config)).isEqualTo(new String[] {"a", "b"});
  }

  @Test
  void combineProtocolsReturnPrefersSSLServerProtocols() {
    final SSLConfig config = mock(SSLConfig.class);
    when(config.getClientProtocolsAsStringArray()).thenReturn(new String[] {"a", "c"});
    when(config.getServerProtocolsAsStringArray()).thenReturn(new String[] {"a", "b"});

    assertThat(combineProtocols(config)).isEqualTo(new String[] {"a", "b", "c"});
  }

  @Test
  void splitReturnsEmptyWhenNull() {
    assertThat(split(null)).isEmpty();
  }

  @Test
  void splitReturnsEmptyWhenEmpty() {
    assertThat(split("")).isEmpty();
  }

  @Test
  void splitReturnsEmptyWhenBlank() {
    assertThat(split("  ")).isEmpty();
  }

  @Test
  void splitReturnsOneWhenSingleValue() {
    assertThat(split("a")).containsExactly("a");
  }

  @Test
  void splitReturnsOneWhenSingleValueTailingSpace() {
    assertThat(split("a ")).containsExactly("a");
  }

  @Test
  void splitReturnsOneWhenSingleValueTrailingComma() {
    assertThat(split("a,")).containsExactly("a");
  }

  @Test
  void splitReturnsTwoWhenSpaceSeparatedValues() {
    assertThat(split("a b")).containsExactly("a", "b");
  }

  @Test
  void splitReturnsTwoWhenCommaSeparatedValues() {
    assertThat(split("a,b")).containsExactly("a", "b");
  }

  @Test
  void splitReturnsTwoWhenCommaWithSpaceSeparatedValues() {
    assertThat(split("a, b")).containsExactly("a", "b");
  }

  @Test
  void splitReturnsThreeWhenMixedCommaAndSpaceSeparatedValues() {
    assertThat(split("a, b c")).containsExactly("a", "b", "c");
  }
}
