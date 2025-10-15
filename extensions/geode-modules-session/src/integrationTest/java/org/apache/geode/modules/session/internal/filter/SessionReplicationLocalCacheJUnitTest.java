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

import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.springframework.mock.web.MockFilterConfig;

import org.apache.geode.modules.session.filter.SessionCachingFilter;
import org.apache.geode.test.junit.categories.SessionTest;
import org.apache.geode.util.internal.GeodeGlossary;

/**
 * This runs all tests with a local cache enabled
 *
 * <p>
 * <b>Jakarta EE 10 Migration Changes:</b>
 * <ul>
 * <li>Migrated from MockRunner to Spring Mock Web framework</li>
 * <li>Direct field access (filterConfig, servletContext, request) instead of WebMockObjectFactory
 * pattern</li>
 * <li>API changes: setInitParameter() → addInitParameter(), setRequestURL() → setRequestURI()</li>
 * </ul>
 */
@Category({SessionTest.class})
public class SessionReplicationLocalCacheJUnitTest extends CommonTests {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();

    // Spring Mock Web: Direct instantiation instead of factory.getMockFilterConfig()
    filterConfig = new MockFilterConfig(servletContext);

    // Spring Mock Web: addInitParameter() replaces setInitParameter()
    filterConfig.addInitParameter(GeodeGlossary.GEMFIRE_PREFIX + "property.mcast-port", "0");
    filterConfig.addInitParameter("cache-type", "peer-to-peer");
    filterConfig.addInitParameter(GeodeGlossary.GEMFIRE_PREFIX + "cache.enable_local_cache",
        "true");

    // Spring Mock Web: Direct field access replaces factory.getMockServletContext()
    servletContext.setContextPath(CONTEXT_PATH);

    // Spring Mock Web: setRequestURI() replaces setRequestURL() (different method name)
    // Direct field access replaces factory.getMockRequest()
    request.setRequestURI("/test/foo/bar");
    request.setContextPath(CONTEXT_PATH);

    createFilter(SessionCachingFilter.class);
    createServlet(CallbackServlet.class);

    setDoChain(true);
  }
}
