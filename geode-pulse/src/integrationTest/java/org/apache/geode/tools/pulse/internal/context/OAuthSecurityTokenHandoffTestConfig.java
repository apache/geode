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

package org.apache.geode.tools.pulse.internal.context;

import static org.mockito.Mockito.mock;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.security.oauth2.client.OAuth2AuthorizedClientService;

import org.apache.geode.tools.pulse.internal.data.ClusterFactory;
import org.apache.geode.tools.pulse.internal.data.Repository;

@Configuration
@Primary
@Profile("pulse.oauth.security.token.test")
public class OAuthSecurityTokenHandoffTestConfig {
  @Bean
  public ClusterFactory clusterFactory() {
    return mock(ClusterFactory.class);
  }

  @Bean
  public Repository repository(OAuth2AuthorizedClientService authorizedClientService,
      ClusterFactory clusterFactory) {
    return new Repository(authorizedClientService, clusterFactory);
  }
}
