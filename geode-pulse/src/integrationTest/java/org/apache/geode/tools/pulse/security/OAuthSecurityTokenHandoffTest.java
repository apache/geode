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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.oauth2.core.OAuth2AccessToken.TokenType.BEARER;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

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
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.oauth2.core.OAuth2AccessToken;
import org.springframework.security.oauth2.core.user.DefaultOAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2User;
import org.springframework.security.oauth2.core.user.OAuth2UserAuthority;
import org.springframework.security.web.context.HttpSessionSecurityContextRepository;
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
  private MockMvc mvc;

  @Autowired
  private ClusterFactory clusterFactory;

  @Autowired
  private OAuth2AuthorizedClientService authorizedClientService;

  private ClientRegistration clientRegistration;

  @Autowired
  private WebApplicationContext context;

  @Before
  public void setup() {
    mvc = MockMvcBuilders
        .webAppContextSetup(context)
        .apply(springSecurity())
        .build();
    clientRegistration = ClientRegistration
        .withRegistrationId("uaa")
        .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
        .redirectUriTemplate("{baseUrl}/oauth2/code/{registrationId}")
        .clientId("pulse")
        .clientSecret("secret")
        .authorizationUri("http://example.com/uaa/oauth/authorize")
        .tokenUri("http://example.com/uaa/oauth/token")
        .userInfoUri("http://example.com/uaa/userinfo")
        .jwkSetUri("http://example.com/uaa/token_keys")
        .clientName("Pulse")
        .userNameAttributeName("sub")
        .build();
  }

  @Test
  public void usesCurrentSessionAccessTokenAsCredentialToConnectToGemFire() throws Exception {
    Cluster cluster = mock(Cluster.class);
    when(clusterFactory.create(any(), any(), any(), any(), any())).thenReturn(cluster);

    OAuth2AuthenticationToken authenticationToken = buildPrincipal();

    OAuth2AccessToken accessToken =
        new OAuth2AccessToken(BEARER, "the-token-value", Instant.now(),
            Instant.now().plus(Duration.ofHours(1)));
    OAuth2AuthorizedClient authorizedClient =
        new OAuth2AuthorizedClient(clientRegistration, authenticationToken.getPrincipal().getName(),
            accessToken);
    authorizedClientService.saveAuthorizedClient(authorizedClient, authenticationToken);

    MockHttpSession session = new MockHttpSession();
    session.setAttribute(
        HttpSessionSecurityContextRepository.SPRING_SECURITY_CONTEXT_KEY,
        new SecurityContextImpl(authenticationToken));

    mvc.perform(get("/dataBrowserRegions").session(session))
        .andExpect(status().is2xxSuccessful());

    verify(clusterFactory).create(any(), any(), any(), any(), any());
    verify(cluster).connectToGemFire("the-token-value");
  }

  private static OAuth2AuthenticationToken buildPrincipal() {
    Map<String, Object> attributes = new HashMap<>();
    attributes.put("sub", "dale-subject");
    attributes.put("email", "dale@dale-test.org");

    List<GrantedAuthority> authorities = Arrays.asList(
        new OAuth2UserAuthority("ROLE_USER", attributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:READ", attributes),
        new OAuth2UserAuthority("SCOPE_CLUSTER:WRITE", attributes),
        new OAuth2UserAuthority("SCOPE_DATA:READ", attributes),
        new OAuth2UserAuthority("SCOPE_DATA:WRITE", attributes));
    OAuth2User user = new DefaultOAuth2User(authorities, attributes, "sub");
    return new OAuth2AuthenticationToken(user, authorities, "uaa");
  }
}
