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

package org.apache.geode.tools.pulse.security;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.oauth2.core.AuthorizationGrantType.AUTHORIZATION_CODE;
import static org.springframework.security.oauth2.core.OAuth2AccessToken.TokenType.BEARER;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.security.web.context.HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.mock.web.MockHttpSession;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextImpl;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClient;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.authentication.OAuth2AuthenticationToken;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.oidc.OidcIdToken;
import org.springframework.security.oauth2.core.oidc.user.DefaultOidcUser;
import org.springframework.security.oauth2.core.oidc.user.OidcUser;
import org.springframework.security.oauth2.core.user.OAuth2UserAuthority;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import org.apache.geode.tools.pulse.internal.data.Cluster;
import org.apache.geode.tools.pulse.internal.data.ClusterFactory;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath*:WEB-INF/pulse-servlet.xml")
@ActiveProfiles({"pulse.oauth.security.token.test", "pulse.authentication.oauth"})
public class OAuthSecurityTokenHandoffTest {
  private static final String AUTHENTICATION_PROVIDER_ID = "uaa";
  private MockMvc mvc;

  @Autowired
  private ClusterFactory clusterFactory;

  @Autowired
  private OAuth2AuthorizedClientService authorizedClientService;

  @Autowired
  private WebApplicationContext context;

  @Before
  public void setup() {
    mvc = MockMvcBuilders
        .webAppContextSetup(context)
        .apply(springSecurity())
        .build();
  }

  @Test
  public void usesCurrentSessionAccessTokenValueAsCredentialToConnectToGemFire() throws Exception {
    String subject = "some-subject";

    Cluster clusterForUser = mock(Cluster.class);
    when(clusterFactory.create(any(), any(), eq(subject), any(), any())).thenReturn(clusterForUser);

    String tokenValue = "the-token-value";
    MockHttpSession session = sessionWithAuthenticatedUser("some-user-name", subject, tokenValue);

    String urlThatTriggersPulseToConnectToGemFire = "/dataBrowserRegions";
    mvc.perform(get(urlThatTriggersPulseToConnectToGemFire).session(session));

    verify(clusterForUser).connectToGemFire(tokenValue);
  }

  private MockHttpSession sessionWithAuthenticatedUser(String userName, String subject,
      String tokenValue) {
    OAuth2AccessToken accessToken = accessToken(tokenValue);
    OidcIdToken idToken = idToken(userName, subject, accessToken);
    OAuth2AuthenticationToken authenticationToken = authenticationToken(idToken);
    authorizeClient(authenticationToken, accessToken);
    return sessionWithAuthenticationToken(authenticationToken);
  }

  private static OAuth2AuthenticationToken authenticationToken(OidcIdToken idToken) {
    List<GrantedAuthority> userAuthorities = allGeodeAuthorities(idToken.getClaims());
    OidcUser user = new DefaultOidcUser(userAuthorities, idToken);
    return new OAuth2AuthenticationToken(user, userAuthorities, AUTHENTICATION_PROVIDER_ID);
  }

  private void authorizeClient(OAuth2AuthenticationToken authenticationToken,
      OAuth2AccessToken accessToken) {
    String userName = authenticationToken.getPrincipal().getName();
    OAuth2AuthorizedClient authorizedClient =
        new OAuth2AuthorizedClient(clientRegistration(), userName, accessToken);
    authorizedClientService.saveAuthorizedClient(authorizedClient, authenticationToken);
  }

  private static OidcIdToken idToken(String userName, String subject,
      OAuth2AccessToken accessToken) {
    return OidcIdToken.withTokenValue(accessToken.getTokenValue())
        .subject(subject)
        .claim("user_name", userName)
        .issuedAt(accessToken.getIssuedAt())
        .expiresAt(accessToken.getExpiresAt())
        .build();
  }

  private static OAuth2AccessToken accessToken(String tokenValue) {
    Instant issuedAt = Instant.now();
    Instant expiresAt = issuedAt.plus(Duration.ofDays(12));
    return new OAuth2AccessToken(BEARER, tokenValue, issuedAt, expiresAt);
  }

  private static List<GrantedAuthority> allGeodeAuthorities(Map<String, Object> userAttributes) {
    return Arrays.asList(
        new OAuth2UserAuthority("ROLE_USER", userAttributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:READ", userAttributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:WRITE", userAttributes),
        new OAuth2UserAuthority("SCOPE_DATA:READ", userAttributes),
        new OAuth2UserAuthority("SCOPE_DATA:WRITE", userAttributes));
  }

  private static ClientRegistration clientRegistration() {
    return ClientRegistration
        .withRegistrationId(AUTHENTICATION_PROVIDER_ID)
        .authorizationGrantType(AUTHORIZATION_CODE)
        .redirectUri("{baseUrl}/oauth2/code/{registrationId}")
        .clientId("client-id")
        .authorizationUri("authorization-uri")
        .tokenUri("token-uri")
        .build();
  }

  private static MockHttpSession sessionWithAuthenticationToken(
      OAuth2AuthenticationToken authenticationToken) {
    MockHttpSession session = new MockHttpSession();
    session.setAttribute(SPRING_SECURITY_CONTEXT_KEY, new SecurityContextImpl(authenticationToken));
    return session;
  }
}
