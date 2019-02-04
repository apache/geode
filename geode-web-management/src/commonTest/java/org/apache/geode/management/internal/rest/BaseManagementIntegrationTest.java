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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.GenericXmlWebContextLoader;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.context.web.WebMergedContextConfiguration;
import org.springframework.test.web.servlet.request.RequestPostProcessor;
import org.springframework.web.context.WebApplicationContext;
import org.springframework.web.context.support.GenericWebApplicationContext;

import org.apache.geode.internal.cache.HttpService;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath*:WEB-INF/geode-management-servlet.xml"},
    loader = TestContextLoader.class)
@WebAppConfiguration
public abstract class BaseManagementIntegrationTest {

  static RequestPostProcessor POST_PROCESSOR =
      new StandardRequestPostProcessor();

  static LocatorStarterRule locator;

  @Autowired
  WebApplicationContext webApplicationContext;

  @BeforeClass
  public static void beforeClass() {
    locator.before();
  }

  @AfterClass
  public static void afterClass() {
    locator.after();
  }
}


class TestContextLoader extends GenericXmlWebContextLoader {
  @Override
  protected void loadBeanDefinitions(GenericWebApplicationContext context,
      WebMergedContextConfiguration webMergedConfig) {
    super.loadBeanDefinitions(context, webMergedConfig);
    context.getServletContext().setAttribute(HttpService.SECURITY_SERVICE_SERVLET_CONTEXT_PARAM,
        BaseManagementIntegrationTest.locator.getCache().getSecurityService());
    context.getServletContext().setAttribute(HttpService.CLUSTER_MANAGEMENT_SERVICE_CONTEXT_PARAM,
        BaseManagementIntegrationTest.locator.getLocator().getClusterManagementService());
  }

}
