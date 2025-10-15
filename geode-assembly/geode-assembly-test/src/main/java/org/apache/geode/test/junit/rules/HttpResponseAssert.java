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

package org.apache.geode.test.junit.rules;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.logging.log4j.Logger;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.assertj.core.api.ListAssert;

import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.api.ClusterManagementRealizationResult;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.configuration.AbstractConfiguration;

/**
 * Apache HttpComponents 5.x migration changes:
 * - HttpResponse → ClassicHttpResponse (more specific interface for classic HTTP/1.1)
 * - getStatusLine().getStatusCode() → getCode() (simplified status code access)
 * - getEntity().getContentType().getValue() → getEntity().getContentType() (returns String
 * directly)
 */
public class HttpResponseAssert
    extends AbstractAssert<HttpResponseAssert, ClassicHttpResponse> {
  private static final Logger logger = LogService.getLogger();
  private final String responseBody;
  private final String logMessage;

  public HttpResponseAssert(String uri, ClassicHttpResponse httpResponse) {
    super(httpResponse, HttpResponseAssert.class);
    try {
      responseBody = getResponseBody();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    if (uri != null) {
      logMessage = uri + ", response body: \n" + responseBody;

    } else {
      logMessage = "response body: \n" + responseBody;
    }
    logger.info(logMessage);
  }

  public static HttpResponseAssert assertResponse(ClassicHttpResponse response) {
    return new HttpResponseAssert(null, response);
  }

  public HttpResponseAssert hasStatusCode(int... httpStatus) {
    // HttpClient 5.x: getStatusLine().getStatusCode() replaced with getCode()
    int statusCode = actual.getCode();
    assertThat(statusCode)
        .describedAs(logMessage + "\n" + descriptionText())
        .isIn(Arrays.stream(httpStatus).boxed().collect(Collectors.toList()));
    return this;
  }

  public AbstractCharSequenceAssert<?, String> hasHeaderValue(String headerName) {
    return assertThat(actual.getFirstHeader(headerName).getValue());
  }

  public AbstractCharSequenceAssert<?, String> hasResponseBody() {
    return assertThat(responseBody);
  }

  public <R extends AbstractConfiguration> ClusterManagementResult getClusterManagementResult()
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(responseBody, ClusterManagementResult.class);
  }

  public <R extends AbstractConfiguration> ClusterManagementRealizationResult getClusterManagementRealizationResult()
      throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(responseBody, ClusterManagementRealizationResult.class);
  }

  public HttpResponseAssert hasContentType(String contentType) {
    // HttpClient 5.x: getContentType() returns String directly
    assertThat(actual.getEntity().getContentType()).containsIgnoringCase(contentType);
    return this;
  }

  public HttpResponseAssert statusIsOk() {
    // HttpClient 5.x: getStatusLine().getStatusCode() replaced with getCode()
    assertThat(actual.getCode())
        .describedAs(logMessage + "\n" + descriptionText())
        .isBetween(200, 299);
    return this;
  }

  public JsonNode getJsonObject() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readTree(responseBody);
  }

  public ListAssert<Double> hasJsonArrayOfDoubles() throws IOException {
    JsonNode array = getJsonObject();
    List<Double> list = new ArrayList<>();

    for (int i = 0; i < array.size(); i++) {
      list.add(array.get(i).doubleValue());
    }
    return assertThat(list);
  }

  private String getResponseBody() throws IOException {
    if (actual.getEntity() == null) {
      return "";
    }
    return IOUtils.toString(actual.getEntity().getContent(), StandardCharsets.UTF_8);
  }

}
