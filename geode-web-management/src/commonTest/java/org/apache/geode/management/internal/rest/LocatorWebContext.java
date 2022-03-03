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

package org.apache.geode.management.internal.rest;

import static org.springframework.security.test.web.servlet.setup.SecurityMockMvcConfigurers.springSecurity;

import org.springframework.test.web.client.MockMvcClientHttpRequestFactory;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.RequestBuilder;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;


/**
 * use in conjunction with any BaseLocatorContextLoader to ease the interaction with the locator
 * started when loading the servlet xml to create the web context.
 */
public class LocatorWebContext {
  static RequestPostProcessor POST_PROCESSOR = new StandardRequestPostProcessor();
  private final WebApplicationContext webApplicationContext;
  private final MockMvc mockMvc;
  private final MockMvcClientHttpRequestFactory requestFactory;


  /**
   * this constructor needs to be called in the @Before method of the test since only at that time
   * the webApplicationContext loaded by the context loader is available.
   */
  public LocatorWebContext(WebApplicationContext webApplicationContext) {
    this.webApplicationContext = webApplicationContext;
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext)
        .apply(springSecurity())
        .addFilter(new ManagementLoggingFilter())
        .build();

    requestFactory = new MockMvcClientHttpRequestFactory(mockMvc);
  }

  public GeodeComponent getLocator() {
    return (GeodeComponent) webApplicationContext.getServletContext().getAttribute("locator");
  }

  public MockMvcClientHttpRequestFactory getRequestFactory() {
    return requestFactory;
  }

  public ResultActions perform(RequestBuilder builder) throws Exception {
    ((MockHttpServletRequestBuilder) builder).with(POST_PROCESSOR);
    return mockMvc.perform(builder);
  }
}
