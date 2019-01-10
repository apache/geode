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

package org.apache.geode.management.internal.rule;

import java.util.function.Supplier;

import javax.servlet.ServletContext;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.servlet.handler.MappedInterceptor;

import org.apache.geode.internal.security.SecurityService;
import org.apache.geode.management.internal.JettyHelper;
import org.apache.geode.management.internal.web.controllers.support.LoginHandlerInterceptor;
import org.apache.geode.test.junit.rules.serializable.SerializableExternalResource;

public class AdminRestStartupRule extends SerializableExternalResource {

  private MockMvc mockMvc;
  private Supplier<WebApplicationContext> contextSupplier;
  private Supplier<SecurityService> securityServiceSupplier;

  public AdminRestStartupRule(Supplier<WebApplicationContext> contextSupplier,
      Supplier<SecurityService> securityServiceSupplier) {
    this.contextSupplier = contextSupplier;
    this.securityServiceSupplier = securityServiceSupplier;
  }

  private static RequestPostProcessor POST_PROCESSOR = new StandardRequestPostProcessor();

  private static class StandardRequestPostProcessor implements RequestPostProcessor {
    @Override
    public MockHttpServletRequest postProcessRequest(MockHttpServletRequest request) {
      request.addHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);
      request.addHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
      return request;
    }
  }

  @Override
  public void before() {
    WebApplicationContext context = contextSupplier.get();
    ServletContext servletContext = context.getServletContext();
    servletContext
        .setAttribute(JettyHelper.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
            securityServiceSupplier.get());
    LoginHandlerInterceptor loginInterceptorHandler =
        (LoginHandlerInterceptor) ((MappedInterceptor) context
            .getBean("org.springframework.web.servlet.handler.MappedInterceptor#1"))
                .getInterceptor();
    loginInterceptorHandler.setServletContext(servletContext);

    mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
  }

  public ResultActions perform(MockHttpServletRequestBuilder requestBuilder) throws Exception {
    requestBuilder.with(POST_PROCESSOR);
    return mockMvc.perform(requestBuilder);
  }
}
