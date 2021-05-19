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

package org.apache.geode.redis.session.springRedisTestApplication.config;

import java.time.Duration;
import java.util.Arrays;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import org.springframework.boot.ApplicationArguments;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.session.data.redis.config.ConfigureRedisAction;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
public class WebMvcConfig implements WebMvcConfigurer {

  @Bean
  public static ConfigureRedisAction configureRedisAction() {
    return ConfigureRedisAction.NO_OP;
  }

  @Bean
  public LettuceConnectionFactory connectionFactory(ApplicationArguments applicationArguments) {
    RedisClusterConfiguration redisConfiguration =
        new RedisClusterConfiguration(Arrays.asList(applicationArguments.getSourceArgs()));
    return new LettuceConnectionFactory(redisConfiguration, lettuceClientConfiguration());
  }

  private LettuceClientConfiguration lettuceClientConfiguration() {
    ClusterTopologyRefreshOptions refreshOptions =
        ClusterTopologyRefreshOptions.builder()
            .enableAllAdaptiveRefreshTriggers()
            .enablePeriodicRefresh(Duration.ofSeconds(5))
            .build();

    return LettuceClientConfiguration.builder()
        .clientOptions(ClusterClientOptions.builder()
            .topologyRefreshOptions(refreshOptions)
            .build())
        .build();
  }

}
