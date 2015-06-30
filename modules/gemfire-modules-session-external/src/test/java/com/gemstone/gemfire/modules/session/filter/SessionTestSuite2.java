/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.filter;

import com.gemstone.gemfire.modules.session.junit.SeparateClassloaderTestRunner;
import com.mockrunner.mock.web.MockFilterConfig;
import com.mockrunner.mock.web.WebMockObjectFactory;
import com.mockrunner.servlet.ServletTestModule;

import java.io.File;
import javax.servlet.Filter;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;

/**
 *
 */
@RunWith(SeparateClassloaderTestRunner.class)
public class SessionTestSuite2 extends GemfireCacheTest {

  private static Filter filter;

  private static final File tmpdir;

  private static final String gemfire_log;

  static {
    // Create a per-user scratch directory
    tmpdir = new File(System.getProperty("java.io.tmpdir"),
        "gemfire_modules-" + System.getProperty("user.name"));
    tmpdir.mkdirs();
    tmpdir.deleteOnExit();

    gemfire_log = tmpdir.getPath() +
        System.getProperty("file.separator") + "gemfire_modules.log";
  }

  @BeforeClass
  public static void setupClass() throws Exception {
    System.out.println("Executing " + SessionTestSuite2.class.getName());
    WebMockObjectFactory factory = new WebMockObjectFactory();
    MockFilterConfig config = factory.getMockFilterConfig();

    config.setInitParameter("mcast-port", "19991");
    config.setInitParameter("gemfire.property.log-file", gemfire_log);
    config.setInitParameter("gemfire.property.writable-working-dir",
        tmpdir.getPath());
    config.setInitParameter("cache-type", "peer-to-peer");
    config.setInitParameter("gemfire.cache.enable_local_cache", "true");

    factory.getMockServletContext().setContextPath("");

    ServletTestModule module = new ServletTestModule(factory);
    filter = module.createFilter(SessionCachingFilter.class);
  }

  @AfterClass
  public static void teardownClass() throws Exception {
    filter.destroy();
  }
}