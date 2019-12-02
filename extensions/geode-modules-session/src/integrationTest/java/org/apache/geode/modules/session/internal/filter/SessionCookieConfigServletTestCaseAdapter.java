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

package org.apache.geode.modules.session.internal.filter;

import javax.servlet.SessionCookieConfig;

import com.mockrunner.mock.web.MockServletContext;
import com.mockrunner.mock.web.MockSessionCookieConfig;
import com.mockrunner.mock.web.WebMockObjectFactory;
import com.mockrunner.servlet.BasicServletTestCaseAdapter;

/**
 * Extend the BasicServletTestCaseAdapter with support for a
 * SessionCookieConfig in the ServletContext.
 */
public class SessionCookieConfigServletTestCaseAdapter
    extends BasicServletTestCaseAdapter {

  public SessionCookieConfigServletTestCaseAdapter() {
    super();
  }

  public SessionCookieConfigServletTestCaseAdapter(String name) {
    super(name);
  }

  @Override
  protected WebMockObjectFactory createWebMockObjectFactory() {
    // create special SessionCookieConfig aware factory
    return new MyWebMockObjectFactory();
  }

  @Override
  protected WebMockObjectFactory createWebMockObjectFactory(
      WebMockObjectFactory otherFactory) {
    // create special SessionCookieConfig aware factory
    return new MyWebMockObjectFactory(otherFactory);
  }

  @Override
  protected WebMockObjectFactory createWebMockObjectFactory(
      WebMockObjectFactory otherFactory, boolean createNewSession) {
    // create special SessionCookieConfig aware factory
    return new MyWebMockObjectFactory(otherFactory, createNewSession);
  }

  /**
   * MockServletContext that has a SessionCookieConfig.
   */
  public static class MyMockServletContext extends MockServletContext {

    private SessionCookieConfig sessionCookieConfig;

    private MyMockServletContext() {
      super();
      sessionCookieConfig = new MyMockSessionCookieConfig();
    }

    @Override
    public synchronized void resetAll() {
      super.resetAll();
      sessionCookieConfig = new MyMockSessionCookieConfig();
    }

    @Override
    public SessionCookieConfig getSessionCookieConfig() {
      return sessionCookieConfig;
    }

  }

  // why doesn't MockSessionCookieConfig implement SessionCookieConfig...
  private static class MyMockSessionCookieConfig extends
      MockSessionCookieConfig implements SessionCookieConfig {
  }

  /**
   * WebMockObjectFactory that creates our SessionCookieConfig aware
   * MockSerletContext.
   */
  public static class MyWebMockObjectFactory extends WebMockObjectFactory {
    public MyWebMockObjectFactory() {
      super();
    }

    public MyWebMockObjectFactory(WebMockObjectFactory factory) {
      super(factory);
    }

    public MyWebMockObjectFactory(WebMockObjectFactory factory, boolean createNewSession) {
      super(factory, createNewSession);
    }

    @Override
    public MyMockServletContext createMockServletContext() {
      return new MyMockServletContext();
    }

  }

}
