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
package org.apache.geode.internal.inet;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class LocalHostUtilTest {

  @Test
  public void isWildCardAddressReturnsTrueIfAddressIsIPv4WildcardIP() {
    assertThat(LocalHostUtil.isWildcardAddress("0.0.0.0")).isTrue();
  }

  @Test
  public void isWildCardAddressReturnsTrueIfAddressIsIPv6WildcardIP() {
    assertThat(LocalHostUtil.isWildcardAddress("::")).isTrue();
  }

  @Test
  public void isWildCardAddressReturnsTrueIfAddressWildcardICharacter() {
    assertThat(LocalHostUtil.isWildcardAddress("*")).isTrue();
  }

  @Test
  public void isWildCardAddressReturnsFalseIfAddressIsNull() {
    assertThat(LocalHostUtil.isWildcardAddress(null)).isFalse();
  }

  @Test
  public void isWildCardAddressReturnsFalseIfAddressIsEmpty() {
    assertThat(LocalHostUtil.isWildcardAddress("")).isFalse();
  }

  @Test
  public void isWildCardAddressReturnsFalseIfAddressIsNotWildcard() {
    assertThat(LocalHostUtil.isWildcardAddress("1.2.3.4")).isFalse();
  }

  @Test
  public void isWildcardCharacterReturnsTrueIfAddressIsWildcardCharacter() {
    assertThat(LocalHostUtil.isWildcardCharacter("*")).isTrue();
  }

  @Test
  public void isWildcardCharacterReturnsFalseIfAddressIsNotWildcardCharacter() {
    assertThat(LocalHostUtil.isWildcardCharacter("1.2.3.4")).isFalse();
  }
}
