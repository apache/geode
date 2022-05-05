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

import static org.apache.geode.internal.net.SSLUtil.DEFAULT_ALGORITHMS;
import static org.apache.geode.internal.net.SSLUtil.split;
import static org.assertj.core.api.Assertions.assertThat;

import java.security.NoSuchAlgorithmException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import org.junit.jupiter.api.Test;


public class SSLUtilTest {

  @Test
  public void getSSLContextInstanceUsesFirstSupportedDefaultAlgorithm()
      throws NoSuchAlgorithmException {
    assertThat(SSLUtil.getSSLContextInstance().getProtocol()).isIn(DEFAULT_ALGORITHMS);
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
