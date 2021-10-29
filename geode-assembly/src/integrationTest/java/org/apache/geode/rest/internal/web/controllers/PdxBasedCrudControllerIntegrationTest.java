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
package org.apache.geode.rest.internal.web.controllers;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_BIND_ADDRESS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_DEV_REST_API;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import javax.annotation.Resource;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.util.StringUtils;
import org.springframework.web.client.RestTemplate;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.GemFireVersion;
import org.apache.geode.management.internal.AgentUtil;
import org.apache.geode.test.junit.categories.RestAPITest;
import org.apache.geode.test.junit.rules.RequiresGeodeHome;

@Category({RestAPITest.class})
public class PdxBasedCrudControllerIntegrationTest {
  @Rule
  public RequiresGeodeHome requiresGeodeHome = new RequiresGeodeHome();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  protected static int DEFAULT_HTTP_SERVICE_PORT = 8189;

  protected static final String PORT_AND_ONE_STRING_FORMAT = "http://localhost:%1$d/geode/v1/%2$s";

  protected static final String PORT_AND_TWO_STRINGS_FORMAT =
      "http://localhost:%1$d/geode/v1/%2$s/%3$s";

  int port = 0;

  @Autowired
  private Cache cache;

  @Resource(name = "gemfireProperties")
  private Properties gemfireProperties;

  @Resource(name = "Region")
  private Region<String, String> region;

  @Before
  public void setupGemFire() {
    AgentUtil agentUtil = new AgentUtil(GemFireVersion.getGemFireVersion());
    assertThat(agentUtil.findWarLocation("geode-web-api"))
        .as("unable to locate geode-web-api WAR file")
        .isNotNull();

    if (cache == null) {
      gemfireProperties = (gemfireProperties != null ? gemfireProperties : new Properties());

      try {
        port = Integer
            .parseInt(StringUtils.trimWhitespace(gemfireProperties.getProperty(HTTP_SERVICE_PORT)));
      } catch (NumberFormatException ignore) {
        int httpServicePort1 = AvailablePortHelper.getRandomAvailableTCPPort();
        int httpServicePort = (httpServicePort1 > 1024 && httpServicePort1 < 65536
            ? httpServicePort1 : DEFAULT_HTTP_SERVICE_PORT);
        gemfireProperties.setProperty(HTTP_SERVICE_PORT, String.valueOf(httpServicePort));
        port = httpServicePort;
      }

      cache = new CacheFactory().set("name", getClass().getSimpleName()).set(MCAST_PORT, "0")
          .set(LOG_LEVEL, "config").set(HTTP_SERVICE_BIND_ADDRESS, "localhost")
          .set(HTTP_SERVICE_PORT, String.valueOf(port))
          .set(START_DEV_REST_API, Boolean.toString(true)).create();

      RegionFactory<String, String> regionFactory = cache.createRegionFactory();
      regionFactory.setDataPolicy(DataPolicy.REPLICATE);
      regionFactory.setKeyConstraint(String.class);
      regionFactory.setValueConstraint(String.class);

      region = regionFactory.create("Region");
      region.put("0", formatJson("zero", "cero"));
      region.put("1", formatJson("one", "uno"));
      region.put("2", formatJson("two", "dos"));
      region.put("3", formatJson("three", "tres"));
    }
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test
  public void testRegionEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate.getForObject(
        String.format(PORT_AND_ONE_STRING_FORMAT, port, region.getName()), String.class);
    assertThat(result).isNotNull();
    assertThat(result.replaceAll("\r", "")).contains("{\n" + "  \"Region\" : [ ");
  }

  @Test
  public void testKeysEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate.getForObject(
        String.format(PORT_AND_TWO_STRINGS_FORMAT, port, region.getName(), "keys"), String.class);
    assertThat(result).isNotNull();
    assertThat(result.replaceAll("\r", "")).contains("{\n  \"keys\" : [ ");
  }

  @Test
  public void testGetKeyEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate.getForObject(
        String.format(PORT_AND_TWO_STRINGS_FORMAT, port, region.getName(), "0"), String.class);
    assertThat(result).isNotNull();
    assertThat(result).contains("\\\"zero\\\"");
  }

  @Test
  public void testPutKeyEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity<String> entity = new HttpEntity<>(formatJson("four", "cuatro"), headers);
    restTemplate.put(String.format(PORT_AND_TWO_STRINGS_FORMAT, port, region.getName(), "4"),
        entity);
    assertThat(region.containsKey("4")).isTrue();
  }

  @Test
  public void testDeleteKeyEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    HttpHeaders headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    HttpEntity<String> entity = new HttpEntity<>(formatJson("four", "cuatro"), headers);
    restTemplate.put(String.format(PORT_AND_TWO_STRINGS_FORMAT, port, region.getName(), "4"),
        entity);
    restTemplate.delete(String.format(PORT_AND_TWO_STRINGS_FORMAT, port, region.getName(), "4"));
    assertThat(region.containsKey("4")).isFalse();
  }

  @Test
  public void testQueriesEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate
        .getForObject(String.format(PORT_AND_ONE_STRING_FORMAT, port, "queries"), String.class);
    assertThat(result).isNotNull();
    assertThat(result.replaceAll("\r", "")).isEqualTo("{\n  \"queries\" : [ ]\n}");
  }

  @Test
  public void testFunctionsEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate
        .getForObject(String.format(PORT_AND_ONE_STRING_FORMAT, port, "functions"), String.class);
    assertThat(result).isNotNull();
    assertThat(result.replaceAll("\r", "")).isEqualTo("{\n  \"functions\" : [ ]\n}");
  }

  @Test
  public void testPingEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate
        .getForObject(String.format(PORT_AND_ONE_STRING_FORMAT, port, "ping"), String.class);
    assertThat(result).isNull();
  }

  @Test
  public void testServersEndpoint() throws Exception {
    final RestTemplate restTemplate = new RestTemplate();
    final String result = restTemplate
        .getForObject(String.format(PORT_AND_ONE_STRING_FORMAT, port, "servers"), String.class);
    assertThat(result).isEqualTo("[ \"http://localhost:" + port + "\" ]");
  }

  private String formatJson(String english, String spanish) {
    StringBuilder builder = new StringBuilder();
    builder.append('{');
    builder.append('\n');
    builder.append("    ");
    builder.append('"');
    builder.append("English");
    builder.append('"');
    builder.append(':');
    builder.append('"');
    builder.append(english);
    builder.append('"');
    builder.append(',');
    builder.append('\n');
    builder.append("    ");
    builder.append('"');
    builder.append("Spanish");
    builder.append('"');
    builder.append(':');
    builder.append('"');
    builder.append(spanish);
    builder.append('"');
    builder.append('\n');
    builder.append('}');
    return builder.toString();
  }
}
