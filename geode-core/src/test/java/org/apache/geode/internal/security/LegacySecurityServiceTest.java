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

package org.apache.geode.internal.security;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class LegacySecurityServiceTest {
  private LegacySecurityService service;

  @Test
  public void emptyConstructor() throws Exception {
    service = new LegacySecurityService();
    assertThat(service.isIntegratedSecurity()).isFalse();
    assertThat(service.isClientSecurityRequired()).isFalse();
    assertThat(service.isPeerSecurityRequired()).isFalse();
    assertThat(service.getPostProcessor()).isNull();
    assertThat(service.getSecurityManager()).isNull();
  }

  @Test
  public void clientAuthenticator() throws Exception {
    service = new LegacySecurityService("abc.create", null);
    assertThat(service.isIntegratedSecurity()).isFalse();
    assertThat(service.isClientSecurityRequired()).isTrue();
    assertThat(service.isPeerSecurityRequired()).isFalse();
    assertThat(service.getPostProcessor()).isNull();
    assertThat(service.getSecurityManager()).isNull();
  }

  @Test
  public void peerAuthenticator() throws Exception {
    service = new LegacySecurityService(null, "abc.create");
    assertThat(service.isIntegratedSecurity()).isFalse();
    assertThat(service.isClientSecurityRequired()).isFalse();
    assertThat(service.isPeerSecurityRequired()).isTrue();
    assertThat(service.getPostProcessor()).isNull();
    assertThat(service.getSecurityManager()).isNull();
  }

}
