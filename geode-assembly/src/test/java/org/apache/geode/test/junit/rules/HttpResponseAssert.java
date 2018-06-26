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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.logging.log4j.Logger;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.AbstractCharSequenceAssert;
import org.assertj.core.api.ListAssert;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.ArrayUtils;

public class HttpResponseAssert extends AbstractAssert<HttpResponseAssert, HttpResponse> {
  private static Logger logger = LogService.getLogger();
  private String responseBody;
  private String logMessage;

  public HttpResponseAssert(String uri, HttpResponse httpResponse) {
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

  public static HttpResponseAssert assertResponse(HttpResponse response) {
    return new HttpResponseAssert(null, response);
  }

  public HttpResponseAssert hasStatusCode(int... httpStatus) {
    int statusCode = actual.getStatusLine().getStatusCode();
    assertThat(statusCode)
        .describedAs(logMessage + "\n" + descriptionText())
        .isIn(ArrayUtils.toIntegerArray(httpStatus));
    return this;
  }

  public AbstractCharSequenceAssert<?, String> hasHeaderValue(String headerName) {
    return assertThat(actual.getFirstHeader(headerName).getValue());
  }

  public AbstractCharSequenceAssert<?, String> hasResponseBody() {
    return assertThat(responseBody);
  }

  public HttpResponseAssert hasContentType(String contentType) {
    assertThat(actual.getEntity().getContentType().getValue()).containsIgnoringCase(contentType);
    return this;
  }

  public HttpResponseAssert statusIsOk() {
    assertThat(actual.getStatusLine().getStatusCode())
        .describedAs(logMessage + "\n" + descriptionText())
        .isBetween(200, 299);
    return this;
  }

  public JSONObject getJsonObject() {
    JSONTokener tokener = new JSONTokener(responseBody);
    return new JSONObject(tokener);
  }

  public JSONArray getJsonArray() {
    JSONTokener tokener = new JSONTokener(responseBody);
    JSONArray array = new JSONArray(tokener);
    return array;
  }

  public ListAssert<Object> hasJsonArray() {
    JSONTokener tokener = new JSONTokener(responseBody);
    JSONArray array = new JSONArray(tokener);
    List<Object> list = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      list.add(array.get(i));
    }
    return assertThat(list);
  }

  private String getResponseBody() throws IOException {
    if (actual.getEntity() == null) {
      return "";
    }
    return IOUtils.toString(actual.getEntity().getContent(), "UTF-8");
  }

}
