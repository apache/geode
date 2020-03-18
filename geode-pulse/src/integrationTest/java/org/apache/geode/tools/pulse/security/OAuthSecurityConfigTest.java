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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.web.util.UriComponentsBuilder.fromUriString;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringRunner.class)
@WebAppConfiguration
@ContextConfiguration("classpath*:WEB-INF/pulse-servlet.xml")
@ActiveProfiles({"pulse.authentication.oauth"})
public class OAuthSecurityConfigTest {
  // When Spring needs to authorize the user via OAuth, it redirects to this URL, which Spring
  // defined based on the OAuth provider we configured. This URL then redirects to the authorization
  // URI we configured for that provider.
  private static final String SPRING_AUTHORIZATION_URL_FOR_OAUTH_PROVIDER =
      "http://localhost/oauth2/authorization/%s";

  // The URL to which the OAuth provider will redirect with the authorization code.
  private static final String PULSE_REDIRECT_URL_FOR_OAUTH_PROVIDER =
      "http://localhost/login/oauth2/code/%s";

  private final Properties pulseProperties = new Properties();

  @Autowired
  private WebApplicationContext context;

  private MockMvc mvc;

  @Before
  public void setup() {
    mvc = MockMvcBuilders
        .webAppContextSetup(context)
        .apply(springSecurity())
        .build();

    try (InputStream propertiesStream = getClass().getClassLoader()
        .getResourceAsStream("pulse.properties")) {
      pulseProperties.load(propertiesStream);
    } catch (IOException cause) {
      throw new RuntimeException("Unable to load pulse.properties for test");
    }
  }

  @Test
  public void visitingProtectedUrlRedirectsUnauthenticatedUserToOAuthClientURL() throws Exception {
    String pulseOAuthProvider = property("pulse.oauth.providerId");
    String aProtectedUrl = "/login.html";

    mvc.perform(get(aProtectedUrl))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrl(springAuthorizationUrlFor(pulseOAuthProvider)));
  }

  @Test
  public void oauthClientURLRedirectsToOPulseOAuthAuthorizationURL() throws Exception {
    String pulseOAuthProvider = property("pulse.oauth.providerId");
    String pulseOAuthClientId = property("pulse.oauth.clientId");
    String pulseOAuthAuthorizationUri = property("pulse.oauth.authorizationUri");

    Map<String, String> expectedRedirectParams = new HashMap<>();
    expectedRedirectParams.put("response_type", "code");
    expectedRedirectParams.put("client_id", pulseOAuthClientId);
    expectedRedirectParams.put("redirect_uri", pulseRedirectUrlFor(pulseOAuthProvider));

    mvc.perform(get(springAuthorizationUrlFor(pulseOAuthProvider)))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrlPath(pulseOAuthAuthorizationUri))
        .andExpect(redirectedUrlParams(expectedRedirectParams));
  }

  private static ResultMatcher redirectedUrlPath(String expectedPath) {
    return result -> {
      String redirectedUrlString = result.getResponse().getRedirectedUrl();
      assertThat(redirectedUrlString).isNotNull();
      String actualPath = redirectedUrlString.split("[?#]")[0];
      assertThat(actualPath)
          .as("redirect URL path")
          .startsWith(expectedPath);
    };
  }

  private static ResultMatcher redirectedUrlParams(Map<String, String> expectedParams) {
    return result -> {
      String redirectedUrlString = result.getResponse().getRedirectedUrl();
      assertThat(redirectedUrlString).isNotNull();
      Map<String, String> actualParams = fromUriString(redirectedUrlString)
          .build()
          .getQueryParams()
          .toSingleValueMap();
      assertThat(actualParams)
          .as("redirected URL params")
          .containsAllEntriesOf(expectedParams);
    };
  }

  private String property(String name) {
    return pulseProperties.getProperty(name);
  }

  private static String pulseRedirectUrlFor(String provider) {
    return format(PULSE_REDIRECT_URL_FOR_OAUTH_PROVIDER, provider);
  }

  private static String springAuthorizationUrlFor(String pulseOAuthProvider) {
    return format(SPRING_AUTHORIZATION_URL_FOR_OAUTH_PROVIDER, pulseOAuthProvider);
  }
}
