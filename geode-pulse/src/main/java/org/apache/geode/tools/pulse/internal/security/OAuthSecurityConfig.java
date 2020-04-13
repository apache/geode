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

package org.apache.geode.tools.pulse.internal.security;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.security.config.annotation.method.configuration.EnableGlobalMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.oauth2.client.InMemoryOAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;
import org.springframework.security.oauth2.client.registration.ClientRegistration;
import org.springframework.security.oauth2.client.registration.ClientRegistrationRepository;
import org.springframework.security.oauth2.client.registration.InMemoryClientRegistrationRepository;
import org.springframework.security.oauth2.client.web.AuthenticatedPrincipalOAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.client.web.OAuth2AuthorizedClientRepository;
import org.springframework.security.oauth2.core.AuthorizationGrantType;
import org.springframework.security.web.authentication.logout.LogoutSuccessHandler;

@Configuration
@EnableWebSecurity
@EnableGlobalMethodSecurity(prePostEnabled = true)
@Profile("pulse.authentication.oauth")
@PropertySource("classpath:pulse.properties")
public class OAuthSecurityConfig extends WebSecurityConfigurerAdapter {
  @Value("${pulse.oauth.providerId}")
  private String providerId;
  @Value("${pulse.oauth.providerName}")
  private String providerName;
  @Value("${pulse.oauth.clientId}")
  private String clientId;
  @Value("${pulse.oauth.clientSecret}")
  private String clientSecret;
  @Value("${pulse.oauth.authorizationUri}")
  private String authorizationUri;
  @Value("${pulse.oauth.tokenUri}")
  private String tokenUri;
  @Value("${pulse.oauth.userInfoUri}")
  private String userInfoUri;
  @Value("${pulse.oauth.jwkSetUri}")
  private String jwkSetUri;
  @Value("${pulse.oauth.userNameAttributeName}")
  private String userNameAttributeName;

  @Bean
  public LogoutSuccessHandler logoutHandler() {
    return new LogoutHandler("/login");
  }

  @Override
  protected void configure(HttpSecurity http) throws Exception {
    http.authorizeRequests(authorize -> authorize
        .mvcMatchers("/pulseVersion", "/scripts/**", "/images/**", "/css/**", "/properties/**")
        .permitAll()
        .mvcMatchers("/dataBrowser*", "/getQueryStatisticsGridModel*")
        .access("hasAuthority('SCOPE_CLUSTER:READ') and hasAuthority('SCOPE_DATA:READ')")
        .mvcMatchers("/*")
        .hasAuthority("SCOPE_CLUSTER:READ")
        .anyRequest().authenticated())
        .oauth2Login(oauth -> oauth.defaultSuccessUrl("/clusterDetail.html", true))
        .exceptionHandling(exception -> exception.accessDeniedPage("/accessDenied.html"))
        .logout(logout -> logout
            .logoutUrl("/clusterLogout")
            .logoutSuccessHandler(logoutHandler()))
        .headers(header -> header
            .frameOptions().deny()
            .xssProtection(xss -> xss
                .xssProtectionEnabled(true)
                .block(true))
            .contentTypeOptions())
        .csrf().disable();
  }

  @Bean
  public ClientRegistrationRepository clientRegistrationRepository() {
    return new InMemoryClientRegistrationRepository(clientRegistration());
  }

  @Bean
  public OAuth2AuthorizedClientService authorizedClientService(
      ClientRegistrationRepository clientRegistrationRepository) {
    return new InMemoryOAuth2AuthorizedClientService(clientRegistrationRepository);
  }

  @Bean
  public OAuth2AuthorizedClientRepository authorizedClientRepository(
      OAuth2AuthorizedClientService authorizedClientService) {
    return new AuthenticatedPrincipalOAuth2AuthorizedClientRepository(authorizedClientService);
  }

  private ClientRegistration clientRegistration() {
    return ClientRegistration.withRegistrationId(providerId)
        .authorizationGrantType(AuthorizationGrantType.AUTHORIZATION_CODE)
        .redirectUriTemplate("{baseUrl}/login/oauth2/code/{registrationId}")
        .clientId(clientId)
        .clientSecret(clientSecret)
        .authorizationUri(authorizationUri)
        .tokenUri(tokenUri)
        .userInfoUri(userInfoUri)
        .jwkSetUri(jwkSetUri)
        // When Spring shows the login page, it displays a link to the OAuth provider's
        // authorization URI. Spring uses the value passed to clientName() as the text for that
        // link. We pass the providerName property here, to let the user know which OAuth provider
        // they will be redirected to.
        .clientName(providerName)
        .userNameAttributeName(userNameAttributeName)
        .build();
  }
}
