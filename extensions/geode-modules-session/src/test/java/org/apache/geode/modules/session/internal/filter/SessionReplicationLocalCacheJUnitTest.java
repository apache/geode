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

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.modules.session.filter.SessionCachingFilter;
import org.apache.geode.test.junit.categories.IntegrationTest;
import com.mockrunner.mock.web.MockFilterConfig;
import com.mockrunner.mock.web.WebMockObjectFactory;
import org.junit.Before;
import org.junit.experimental.categories.Category;

/**
 * This runs all tests with a local cache enabled
 */
@Category(IntegrationTest.class)
public class SessionReplicationLocalCacheJUnitTest extends CommonTests {

  @Before
  public void setUp() throws Exception {
    super.setUp();

    WebMockObjectFactory factory = getWebMockObjectFactory();
    MockFilterConfig config = factory.getMockFilterConfig();

    config.setInitParameter(DistributionConfig.GEMFIRE_PREFIX + "property.mcast-port", "0");
    config.setInitParameter("cache-type", "peer-to-peer");
    config.setInitParameter(DistributionConfig.GEMFIRE_PREFIX + "cache.enable_local_cache", "true");

    factory.getMockServletContext().setContextPath(CONTEXT_PATH);

    factory.getMockRequest().setRequestURL("/test/foo/bar");
    factory.getMockRequest().setContextPath(CONTEXT_PATH);

    createFilter(SessionCachingFilter.class);
    createServlet(CallbackServlet.class);

    setDoChain(true);
  }
}
