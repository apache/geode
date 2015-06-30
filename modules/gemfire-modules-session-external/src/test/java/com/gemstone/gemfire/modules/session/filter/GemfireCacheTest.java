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
import com.mockrunner.mock.web.MockServletConfig;
import org.junit.Before;

/**
 * This servlet tests the effects of the downstream SessionCachingFilter filter.
 * When these tests are performed, the filter would already have taken effect.
 */
public class GemfireCacheTest extends CommonTests {

  @Before
  public void setUp() throws Exception {
    super.setUp();
    getWebMockObjectFactory().getMockRequest().setRequestURL("/test/foo/bar");
    getWebMockObjectFactory().getMockRequest().setContextPath(CONTEXT_PATH);
    MockFilterConfig config = getWebMockObjectFactory().getMockFilterConfig();

    config.setInitParameter("gemfire.property.mcast-port", "19991");
    config.setInitParameter("cache-type", "peer-to-peer");
    System.setProperty("gemfire.logdir", "/tmp");

    getWebMockObjectFactory().getMockServletContext().setContextPath(
        CONTEXT_PATH);

    setDoChain(true);

    createFilter(SessionCachingFilter.class);
    createServlet(CallbackServlet.class);

    MockServletConfig servletConfig = getWebMockObjectFactory().getMockServletConfig();
    ContextManager.getInstance().putContext(servletConfig.getServletContext());
  }
}