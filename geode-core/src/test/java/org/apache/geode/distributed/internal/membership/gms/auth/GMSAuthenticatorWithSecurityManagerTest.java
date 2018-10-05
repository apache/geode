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
package org.apache.geode.distributed.internal.membership.gms.auth;

import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_PEER_AUTH_INIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.GemFireSecurityException;
import org.apache.geode.test.junit.categories.SecurityTest;

/**
 * Unit tests GMSAuthenticator using new integrated security.
 */
@Category({SecurityTest.class})
public class GMSAuthenticatorWithSecurityManagerTest extends AbstractGMSAuthenticatorTestCase {

  @Override
  protected boolean isIntegratedSecurity() {
    return true;
  }

  @Test
  public void nullManagerShouldReturnNull() throws Exception {
    assertThat(this.securityProps).doesNotContainKey(SECURITY_MANAGER);
    String result =
        this.authenticator.authenticate(this.member, this.securityProps, this.securityProps);
    assertThat(result).isNull();
  }

  @Test
  public void emptyAuthenticatorShouldReturnNull() throws Exception {
    this.securityProps.setProperty(SECURITY_MANAGER, "");
    String result =
        this.authenticator.authenticate(this.member, this.securityProps, this.securityProps);
    assertThat(result).isNull();
  }

  @Test
  public void shouldGetSecurityPropsFromDistributionConfig() throws Exception {
    this.securityProps.setProperty(SECURITY_PEER_AUTH_INIT, "dummy1");
    this.securityProps.setProperty(SECURITY_MANAGER, "dummy2");

    Properties secProps = this.authenticator.getSecurityProps();

    assertThat(secProps.size()).isEqualTo(2);
    assertThat(secProps.getProperty(SECURITY_PEER_AUTH_INIT)).isEqualTo("dummy1");
    assertThat(secProps.getProperty(SECURITY_MANAGER)).isEqualTo("dummy2");
  }

  @Test
  public void usesPeerAuthInitToGetCredentials() throws Exception {
    this.props.setProperty(SECURITY_PEER_AUTH_INIT, SpyAuthInit.class.getName() + ".create");
    this.props.setProperty(SECURITY_MANAGER, "dummy");

    SpyAuthInit auth = new SpyAuthInit();
    assertThat(auth.isClosed()).isFalse();

    SpyAuthInit.setAuthInitialize(auth);
    Properties credentials = this.authenticator.getCredentials(this.member, this.props);

    assertThat(credentials).isEqualTo(this.props);
    assertThat(auth.isClosed()).isTrue();
    assertThat(SpyAuthInit.getCreateCount() == 1).isTrue();
  }

  @Test
  public void getCredentialsShouldReturnNullIfNoPeerAuthInit() throws Exception {
    Properties credentials = this.authenticator.getCredentials(this.member, this.props);
    assertThat(credentials).isNull();
  }

  @Test
  public void getCredentialsShouldReturnNullIfEmptyPeerAuthInit() throws Exception {
    this.props.setProperty(SECURITY_PEER_AUTH_INIT, "");
    Properties credentials = this.authenticator.getCredentials(this.member, this.props);
    assertThat(credentials).isNull();
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitDoesNotExist() throws Exception {
    String authInit = getClass().getName() + "$NotExistAuth.create";
    this.props.setProperty(SECURITY_PEER_AUTH_INIT, authInit);
    assertThatThrownBy(() -> this.authenticator.getCredentials(this.member, this.props))
        .hasMessageContaining("Instance could not be obtained");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitCreateReturnsNull() throws Exception {
    this.props.setProperty(SECURITY_PEER_AUTH_INIT,
        AuthInitCreateReturnsNull.class.getName() + ".create");
    assertThatThrownBy(() -> this.authenticator.getCredentials(this.member, this.props))
        .hasMessageContaining("Instance could not be obtained from");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitGetCredentialsAndInitThrow() throws Exception {
    this.props.setProperty(SECURITY_PEER_AUTH_INIT,
        AuthInitGetCredentialsAndInitThrow.class.getName() + ".create");
    assertThatThrownBy(() -> this.authenticator.getCredentials(this.member, this.props))
        .hasMessage("expected init error");
  }

  @Test
  public void getCredentialsShouldThrowIfPeerAuthInitGetCredentialsThrows() throws Exception {
    this.props.setProperty(SECURITY_PEER_AUTH_INIT,
        AuthInitGetCredentialsThrows.class.getName() + ".create");
    assertThatThrownBy(() -> this.authenticator.getCredentials(this.member, this.props))
        .hasMessage("expected get credential error");
  }

  @Test
  public void authenticateShouldReturnNullIfSuccessful() throws Exception {
    this.props.setProperty(SECURITY_MANAGER, "dummy");
    String result = this.authenticator.authenticate(this.member, this.props, this.props);
    assertThat(result).isNull();
  }

  @Test
  public void authenticateShouldReturnNullIfNoSecurityManager() throws Exception {
    String result = this.authenticator.authenticate(this.member, this.props, this.props);
    assertThat(result).isNull();
  }

  @Test
  public void authenticateShouldReturnFailureMessageIfLoginThrows() throws Exception {
    when(this.securityService.login(any(Properties.class)))
        .thenThrow(new GemFireSecurityException("dummy"));
    this.props.setProperty(SECURITY_MANAGER, "dummy");
    String result = this.authenticator.authenticate(this.member, this.props, this.props);
    assertThat(result).startsWith("Security check failed. dummy");
  }

  @Test
  public void authenticateShouldReturnFailureMessageIfNullCredentials() throws Exception {
    this.props.setProperty(SECURITY_MANAGER, "dummy");
    String result = this.authenticator.authenticate(this.member, null, this.props);
    assertThat(result).startsWith("Failed to find credentials from");
  }

}
