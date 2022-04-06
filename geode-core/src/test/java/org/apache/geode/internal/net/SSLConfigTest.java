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

import static org.apache.geode.internal.net.SSLConfig.builder;
import static org.apache.geode.internal.net.SSLConfig.isAnyCiphers;
import static org.apache.geode.internal.net.SSLConfig.isAnyProtocols;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SSLConfigTest {

  @Test
  void isAnyProtocolsReturnsTrueForNullString() {
    assertThat(isAnyProtocols((String) null)).isTrue();
  }

  @Test
  void isAnyProtocolsReturnsTrueForEmptyString() {
    assertThat(isAnyProtocols("")).isTrue();
  }

  @Test
  void isAnyProtocolsReturnsTrueForAnyString() {
    assertThat(isAnyProtocols("any")).isTrue();
    assertThat(isAnyProtocols("Any")).isTrue();
    assertThat(isAnyProtocols("ANY")).isTrue();
  }

  @Test
  void isAnyProtocolsReturnsFalseForOtherString() {
    assertThat(isAnyProtocols("other")).isFalse();
  }

  @Test
  void testIsAnyProtocolsForNullArray() {
    assertThat(isAnyProtocols((String[]) null)).isTrue();
  }

  @SuppressWarnings("RedundantArrayCreation")
  @Test
  void isAnyProtocolsReturnsTrueForEmptyArray() {
    assertThat(isAnyProtocols(new String[0])).isTrue();
  }

  @Test
  void isAnyProtocolsReturnsTrueForAnyArray() {
    assertThat(isAnyProtocols(new String[] {"any"})).isTrue();
    assertThat(isAnyProtocols(new String[] {"Any"})).isTrue();
    assertThat(isAnyProtocols(new String[] {"ANY"})).isTrue();
    assertThat(isAnyProtocols("any", "other")).isTrue();
  }

  @Test
  void isAnyProtocolsReturnsFalseForOtherArray() {
    assertThat(isAnyProtocols(new String[] {"other"})).isFalse();
    assertThat(isAnyProtocols("other", "something")).isFalse();
    assertThat(isAnyProtocols("other", "something", "any")).isFalse();
  }

  @Test
  void isAnyCiphersReturnsTrueForNullString() {
    assertThat(isAnyCiphers((String) null)).isTrue();
  }

  @Test
  void isAnyCiphersReturnsTrueForEmptyString() {
    assertThat(isAnyCiphers("")).isTrue();
  }

  @Test
  void isAnyCiphersReturnsTrueForAnyString() {
    assertThat(isAnyCiphers("any")).isTrue();
    assertThat(isAnyCiphers("Any")).isTrue();
    assertThat(isAnyCiphers("ANY")).isTrue();
  }

  @Test
  void isAnyCiphersReturnsFalseForOtherString() {
    assertThat(isAnyCiphers("other")).isFalse();
  }

  @Test
  void testIsAnyCiphersForNullArray() {
    assertThat(isAnyCiphers((String[]) null)).isTrue();
  }

  @SuppressWarnings("RedundantArrayCreation")
  @Test
  void isAnyCiphersReturnsTrueForEmptyArray() {
    assertThat(isAnyCiphers(new String[0])).isTrue();
  }

  @Test
  void isAnyCiphersReturnsTrueForAnyArray() {
    assertThat(isAnyCiphers(new String[] {"any"})).isTrue();
    assertThat(isAnyCiphers(new String[] {"Any"})).isTrue();
    assertThat(isAnyCiphers(new String[] {"ANY"})).isTrue();
    assertThat(isAnyCiphers("any", "other")).isTrue();
  }

  @Test
  void isAnyCiphersReturnsFalseForOtherArray() {
    assertThat(isAnyCiphers(new String[] {"other"})).isFalse();
    assertThat(isAnyCiphers("other", "something")).isFalse();
    assertThat(isAnyCiphers("other", "something", "any")).isFalse();
  }

  @Test
  void getClientProtocolsDefaultsToAny() {
    final SSLConfig config = builder().build();
    assertThat(config.getClientProtocols()).isEqualTo("any");
  }

  @Test
  void getClientProtocolsEqualsSetProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setProtocols(protocols).build();
    assertThat(config.getClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void getClientProtocolsEqualsSetClientProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setClientProtocols(protocols).build();
    assertThat(config.getClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void getClientProtocolsUsesSetClientProtocolsOverSetProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setProtocols("nope").setClientProtocols(protocols).build();
    assertThat(config.getClientProtocols()).isEqualTo(protocols);
  }

  @Test
  void getServerProtocolsDefaultsToAny() {
    final SSLConfig config = builder().build();
    assertThat(config.getServerProtocols()).isEqualTo("any");
  }

  @Test
  void getServerProtocolsEqualsSetProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setProtocols(protocols).build();
    assertThat(config.getServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void getServerProtocolsEqualsSetServerProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setServerProtocols(protocols).build();
    assertThat(config.getServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void getServerProtocolsUsesSetServerProtocolsOverSetProtocols() {
    final String protocols = "SuperProtocol1";
    final SSLConfig config = builder().setProtocols("nope").setServerProtocols(protocols).build();
    assertThat(config.getServerProtocols()).isEqualTo(protocols);
  }

  @Test
  void getClientProtocolsAsStringArrayDefaultsToAny() {
    final SSLConfig config = builder().build();
    assertThat(config.getClientProtocolsAsStringArray()).containsExactly("any");
  }

  @Test
  void getClientProtocolsAsStringArrayEqualsSetProtocols() {
    final SSLConfig config = builder().setProtocols("SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getClientProtocolsAsStringArray()).containsExactly("SuperProtocol1",
        "SuperProtocol2");
  }

  @Test
  void getClientProtocolsAsStringArrayEqualsSetClientProtocols() {
    final SSLConfig config = builder().setClientProtocols("SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getClientProtocolsAsStringArray()).containsExactly(
        "SuperProtocol1", "SuperProtocol2");
  }

  @Test
  void getClientProtocolsAsStringArrayUsesSetClientProtocolsOverSetProtocols() {
    final SSLConfig config = builder().setProtocols("nope").setClientProtocols(
        "SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getClientProtocolsAsStringArray()).containsExactly("SuperProtocol1",
        "SuperProtocol2");
  }

  @Test
  void getServerProtocolsAsStringArrayDefaultsToAny() {
    final SSLConfig config = builder().build();
    assertThat(config.getServerProtocolsAsStringArray()).containsExactly("any");
  }

  @Test
  void getServerProtocolsAsStringArrayAsStringArrayEqualsSetProtocols() {
    final SSLConfig config = builder().setProtocols("SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getServerProtocolsAsStringArray()).containsExactly(
        "SuperProtocol1", "SuperProtocol2");
  }

  @Test
  void getServerProtocolsAsStringArrayEqualsSetServerProtocols() {
    final SSLConfig config = builder().setServerProtocols("SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getServerProtocolsAsStringArray()).containsExactly(
        "SuperProtocol1", "SuperProtocol2");
  }

  @Test
  void getServerProtocolsAsStringArrayUsesSetServerProtocolsOverSetProtocols() {
    final SSLConfig config = builder().setProtocols("nope").setServerProtocols(
        "SuperProtocol1,SuperProtocol2").build();
    assertThat(config.getServerProtocolsAsStringArray()).containsExactly(
        "SuperProtocol1", "SuperProtocol2");
  }

}
