/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.gemstone.gemfire.modules.session.filter;

import com.mockrunner.mock.web.MockFilterConfig;
import com.mockrunner.mock.web.MockHttpServletRequest;
import com.mockrunner.mock.web.MockHttpServletResponse;
import com.mockrunner.mock.web.MockServletConfig;
import com.mockrunner.servlet.BasicServletTestCaseAdapter;
import org.junit.After;
import org.junit.Before;

import javax.servlet.Filter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;

/**
 * This servlet tests the effects of the downstream SessionCachingFilter filter.
 * When these tests are performed, the filter would already have taken effect.
 */
public class GemfireLocalCacheTest extends BasicServletTestCaseAdapter {

  Filter filter;

  protected static final String CONTEXT_PATH = "/test";

  @Before
  public void setUp() throws Exception {
    super.setUp();
    getWebMockObjectFactory().getMockRequest().setRequestURL("/test/foo/bar");
    getWebMockObjectFactory().getMockRequest().setContextPath(CONTEXT_PATH);
    MockFilterConfig config = getWebMockObjectFactory().getMockFilterConfig();

    config.setInitParameter("gemfire.property.mcast-port", "19991");
    config.setInitParameter("cache-type", "peer-to-peer");
    config.setInitParameter("gemfire.cache.enable_local_cache", "true");
    System.setProperty("gemfire.logdir", "/tmp");

    getWebMockObjectFactory().getMockServletContext().setContextPath(
        CONTEXT_PATH);

    setDoChain(true);

    filter = createFilter(SessionCachingFilter.class);
    createServlet(CallbackServlet.class);

    MockServletConfig servletConfig = getWebMockObjectFactory().getMockServletConfig();
    ContextManager.getInstance().putContext(servletConfig.getServletContext());
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
    filter.destroy();
  }

}