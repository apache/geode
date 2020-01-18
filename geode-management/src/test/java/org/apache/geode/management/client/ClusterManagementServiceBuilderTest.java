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

package org.apache.geode.management.client;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Field;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;

import javax.net.ssl.SSLContext;

import org.apache.http.client.RedirectStrategy;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.junit.Test;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.DefaultUriTemplateHandler;

import org.apache.geode.management.api.BaseConnectionConfig;
import org.apache.geode.management.api.ClusterManagementService;

public class ClusterManagementServiceBuilderTest {

  private static final String HOST = "localhost";
  private static final int PORT = 7777;

  private <T> T getFieldValue(Object target, String fieldName) throws NoSuchFieldException {

    Field field = ReflectionUtils.findField(target.getClass(), fieldName);

    return Optional.ofNullable(field)
        .map(it -> {
          ReflectionUtils.makeAccessible(it);
          return field;
        })
        .map(it -> (T) ReflectionUtils.getField(it, target))
        .orElseThrow(() -> new NoSuchFieldException(
            String.format("Field with name [%s] was not found on Object of type [%s]",
                fieldName, target.getClass().getName())));
  }

  @Test
  public void hostAndPortAreSetCorrectly() throws NoSuchFieldException {
    BaseConnectionConfig connectionConfig = new BaseConnectionConfig(HOST, PORT);
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(((DefaultUriTemplateHandler) (restTemplate.getUriTemplateHandler())).getBaseUrl())
        .contains(HOST + ":" + PORT);
  }

  @Test
  public void settingSSLUsesHTTPS() throws NoSuchAlgorithmException, NoSuchFieldException {
    BaseConnectionConfig connectionConfig = new BaseConnectionConfig(HOST, PORT);
    connectionConfig.setSslContext(SSLContext.getDefault());
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(((DefaultUriTemplateHandler) (restTemplate.getUriTemplateHandler())).getBaseUrl())
        .contains("https://");
  }

  @Test
  public void notSettingSSLUsesHTTP() throws NoSuchAlgorithmException, NoSuchFieldException {
    BaseConnectionConfig connectionConfig = new BaseConnectionConfig(HOST, PORT);
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(((DefaultUriTemplateHandler) (restTemplate.getUriTemplateHandler())).getBaseUrl())
        .contains("http://");
  }

  @Test
  public void followRedirectsIsSetWhenEnabled() throws NoSuchFieldException {
    BaseConnectionConfig connectionConfig = new BaseConnectionConfig(HOST, PORT);
    connectionConfig.setFollowRedirects(true);
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    HttpComponentsClientHttpRequestFactory requestFactory =
        (HttpComponentsClientHttpRequestFactory) restTemplate.getRequestFactory();
    Object client = requestFactory.getHttpClient();
    Object config = getFieldValue(client, "execChain");
    RedirectStrategy redirectStrategy = getFieldValue(config, "redirectStrategy");
    assertThat(redirectStrategy).isInstanceOf(LaxRedirectStrategy.class);
  }

  @Test
  public void followRedirectsIsNotSetWhenNotEnabled() throws NoSuchFieldException {
    BaseConnectionConfig connectionConfig = new BaseConnectionConfig(HOST, PORT);
    connectionConfig.setFollowRedirects(false);
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setConnectionConfig(connectionConfig).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    HttpComponentsClientHttpRequestFactory requestFactory =
        (HttpComponentsClientHttpRequestFactory) restTemplate.getRequestFactory();
    Object client = requestFactory.getHttpClient();
    Object execChain = getFieldValue(client, "execChain");
    RedirectStrategy redirectStrategy = getFieldValue(execChain, "redirectStrategy");
    assertThat(redirectStrategy).isInstanceOf(DefaultRedirectStrategy.class);
  }
}
