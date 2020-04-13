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
import java.util.HashMap;
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
import org.springframework.security.oauth2.core.user.DefaultOAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2User;
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
  public void usesCurrentSessionAccessTokenAsCredentialToConnectToGemFire() throws Exception {
    String userName = "some-user-name";
    String accessTokenValue = "the-access-token-value";
    String urlThatTriggersPulseToConnectToGemFire = "/dataBrowserRegions";

    Cluster clusterForUser = mock(Cluster.class);
    when(clusterFactory.create(any(), any(), eq(userName), any(), any()))
        .thenReturn(clusterForUser);

    MockHttpSession session = sessionWithAuthenticatedUser(userName, accessTokenValue);

    mvc.perform(get(urlThatTriggersPulseToConnectToGemFire).session(session));

    verify(clusterForUser).connectToGemFire(accessTokenValue);
  }

  private void authorizeClient(
      OAuth2AuthenticationToken authenticationToken, OAuth2AccessToken accessToken) {
    OAuth2AuthorizedClient authorizedClient =
        new OAuth2AuthorizedClient(clientRegistration(),
            authenticationToken.getPrincipal().getName(), accessToken);
    authorizedClientService.saveAuthorizedClient(authorizedClient, authenticationToken);
  }

  private MockHttpSession sessionWithAuthenticatedUser(String username, String tokenValue) {
    OAuth2AuthenticationToken authenticationToken = authenticationToken(username);
    authorizeClient(authenticationToken, accessToken(tokenValue));
    return sessionWithAuthenticationToken(authenticationToken);
  }

  private static OAuth2AccessToken accessToken(String tokenValue) {
    return new OAuth2AccessToken(BEARER, tokenValue, Instant.now(),
        Instant.now().plus(Duration.ofHours(1)));
  }

  private static OAuth2AuthenticationToken authenticationToken(String userName) {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", userName);

    List<GrantedAuthority> authorities = Arrays.asList(
        new OAuth2UserAuthority("ROLE_USER", attributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:READ", attributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:WRITE", attributes),
        new OAuth2UserAuthority("SCOPE_DATA:READ", attributes),
        new OAuth2UserAuthority("SCOPE_DATA:WRITE", attributes));
    OAuth2User user = new DefaultOAuth2User(authorities, attributes, "sub");
    return new OAuth2AuthenticationToken(user, authorities, AUTHENTICATION_PROVIDER_ID);
  }

  private static ClientRegistration clientRegistration() {
    return ClientRegistration
        .withRegistrationId(AUTHENTICATION_PROVIDER_ID)
        .authorizationGrantType(AUTHORIZATION_CODE)
        .redirectUriTemplate("{baseUrl}/oauth2/code/{registrationId}")
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
