/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.rest.internal.web;


import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)

public class SwaggerVerificationTest extends AbstractRestTest {

  @Test
  public void isSwaggerRunning() throws Exception {
    // Check the UI
    HttpResponse response = doRequest(new HttpGet("/geode/swagger-ui.html"), "", "");
    assertThat(getCode(response), is(200));

    // Check the JSON
    response = doRequest(new HttpGet("/geode/v2/api-docs"), "", "");
    assertThat(getCode(response), is(200));
    JSONObject json = new JSONObject(getResponseBodyJson(response));
    assertThat(json.get("swagger"), is("2.0"));

    JSONObject info = json.getJSONObject("info");
    assertThat(info.getString("description"), is(LocalizedStrings.SwaggerConfig_DESCRIPTOR.toLocalizedString()));
    assertThat(info.getString("title"), is(LocalizedStrings.SwaggerConfig_VENDOR_PRODUCT_LINE.toLocalizedString()));

    JSONObject license = info.getJSONObject("license");
    assertThat(license.getString("name"), is("Apache License, version 2.0"));
    assertThat(license.getString("url"), is("http://www.apache.org/licenses/"));

  }
}
