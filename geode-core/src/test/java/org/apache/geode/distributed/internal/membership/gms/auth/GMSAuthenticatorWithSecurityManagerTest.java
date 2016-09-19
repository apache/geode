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
package org.apache.geode.distributed.internal.membership.gms.auth;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests GMSAuthenticator using new integrated security.
 */
@Category({ UnitTest.class, SecurityTest.class })
public class GMSAuthenticatorWithSecurityManagerTest extends AbstractGMSAuthenticatorTestCase {

  @Override
  protected boolean isIntegratedSecurity() {
    return true;
  }

  @Test
  public void nullManagerShouldReturnNull() throws Exception {
    assertThat(securityProps).doesNotContainKey(SECURITY_MANAGER);
    String result = authenticator.authenticate(member, securityProps, securityProps);
    assertThat(result).isNull();
  }

  @Test
  public void emptyAuthenticatorShouldReturnNull() throws Exception {
    securityProps.setProperty(SECURITY_MANAGER, "");
    String result = authenticator.authenticate(member, securityProps, securityProps);
    assertThat(result).isNull();
  }

  @Test
  public void shouldGetSecurityPropsFromDistributionConfig() throws Exception {
    securityProps.setProperty(SECURITY_PEER_AUTH_INIT, "dummy1");
    securityProps.setProperty(SECURITY_MANAGER, "dummy2");

    Properties secProps = authenticator.getSecurityProps();

    assertThat(secProps.size()).isEqualTo(2);
    assertThat(secProps.getProperty(SECURITY_PEER_AUTH_INIT)).isEqualTo("dummy1");
    assertThat(secProps.getProperty(SECURITY_MANAGER)).isEqualTo("dummy2");
  }

  @Test
  public void usesPeerAuthInitToGetCredentials() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, SpyAuthInit.class.getName() + ".create");
    props.setProperty(SECURITY_MANAGER, "dummy");

    SpyAuthInit auth = new SpyAuthInit();
    assertThat(auth.isClosed()).isFalse();

    SpyAuthInit.setAuthInitialize(auth);
    Properties credentials = authenticator.getCredentials(member, props);

    assertThat(credentials).isEqualTo(props);
    assertThat(auth.isClosed()).isTrue();
    assertThat(SpyAuthInit.getCreateCount() == 1).isTrue();
  }

  @Test
  public void getCredentialsShouldReturnNullIfNoPeerAuthInit() throws Exception {
    Properties credentials = authenticator.getCredentials(member, props);
    assertThat(credentials).isNull();
  }

  @Test
  public void getCredentialsShouldReturnNullIfEmptyPeerAuthInit() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, "");
    Properties credentials = authenticator.getCredentials(member, props);
    assertThat(credentials).isNull();
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitDoesNotExist() throws Exception {
    String authInit = getClass().getName() + "$NotExistAuth.create";
    props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
    assertThatThrownBy(() -> authenticator.getCredentials(member, props)).hasMessageContaining("Instance could not be obtained");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitCreateReturnsNull() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, AuthInitCreateReturnsNull.class.getName() + ".create");
    assertThatThrownBy(() -> authenticator.getCredentials(member, props)).hasMessageContaining("Instance could not be obtained from");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitGetCredentialsAndInitThrow() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, AuthInitGetCredentialsAndInitThrow.class.getName() + ".create");
    assertThatThrownBy(() -> authenticator.getCredentials(member, props)).hasMessage("expected init error");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitGetCredentialsThrows() throws Exception {
    props.setProperty(SECURITY_PEER_AUTH_INIT, AuthInitGetCredentialsThrows.class.getName() + ".create");
    assertThatThrownBy(() -> authenticator.getCredentials(member, props)).hasMessage("expected get credential error");
  }

  @Test
  public void authenticateShouldReturnNullIfSuccessful() throws Exception {
    props.setProperty(SECURITY_MANAGER, "dummy");
    String result = authenticator.authenticate(member, props, props);
    assertThat(result).isNull();
  }

  @Test
  public void authenticateShouldReturnNullIfNoSecurityManager() throws Exception {
    String result = authenticator.authenticate(member, props, props);
    assertThat(result).isNull();
  }

  @Test
  public void authenticateShouldReturnFailureMessageIfLoginThrows() throws Exception {
    when(securityService.login(any(Properties.class))).thenThrow(new GemFireSecurityException("dummy"));
    props.setProperty(SECURITY_MANAGER, "dummy");
    String result = authenticator.authenticate(member, props, props);
    assertThat(result).startsWith("Security check failed. dummy");
  }

  @Test
  public void authenticateShouldReturnFailureMessageIfNullCredentials() throws Exception {
    props.setProperty(SECURITY_MANAGER, "dummy");
    String result = authenticator.authenticate(member, null, props);
    assertThat(result).startsWith("Failed to find credentials from");
  }

}

