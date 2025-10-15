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

import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.springframework.util.ReflectionUtils;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;

public class ClusterManagementServiceBuilderTest {

  private static final String HOST = "localhost";
  private static final int PORT = 7777;

  @Rule
  public final RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @SuppressWarnings("unchecked")
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
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setHost(HOST).setPort(PORT).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(restTemplate.getUriTemplateHandler().expand("").toString())
        .contains(HOST + ":" + PORT);
  }

  @Test
  public void settingSSLUsesHTTPS() throws NoSuchAlgorithmException, NoSuchFieldException {
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder()
            .setHost(HOST)
            .setPort(PORT)
            .setSslContext(SSLContext.getDefault())
            .build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(restTemplate.getUriTemplateHandler().expand("").toString())
        .contains("https://");
  }

  @Test
  public void notSettingSSLUsesHTTP() throws NoSuchFieldException {
    ClusterManagementService cms =
        new ClusterManagementServiceBuilder().setHost(HOST).setPort(PORT).build();

    RestTemplate restTemplate = getFieldValue(getFieldValue(cms, "transport"), "restTemplate");
    assertThat(restTemplate.getUriTemplateHandler().expand("").toString())
        .contains("http://");
  }

  // Note: Tests for followRedirects configuration were removed during HttpClient 5.x migration.
  // The previous tests used reflection to inspect HttpClient 4.x internal fields (execChain,
  // redirectStrategy), which don't exist in HttpClient 5.x due to architectural changes.
  // Redirect behavior is now configured via HttpClientBuilder.disableRedirectHandling()
  // and should be tested through actual HTTP behavior tests rather than internal state inspection.
}
