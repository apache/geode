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
package com.gemstone.gemfire.management.internal;

import static org.junit.Assert.*;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The JettyHelperJUnitTest class is a test suite of test cases testing the
 * contract and functionality of the JettyHelper class. Does not start Jetty.
 *
 * @see com.gemstone.gemfire.management.internal.JettyHelper
 * @see org.jmock.Mockery
 * @see org.junit.Assert
 * @see org.junit.Test
 */
@Category(UnitTest.class)
public class JettyHelperJUnitTest {

  @Test
  public void testSetPortNoBindAddress() throws Exception {

    final Server jetty = JettyHelper.initJetty(null, 8090, false, false, null, null, null);

    assertNotNull(jetty);
    assertNotNull(jetty.getConnectors()[0]);
    assertEquals(8090, ((ServerConnector) jetty.getConnectors()[0]).getPort());
  }

  @Test
  public void testSetPortWithBindAddress() throws Exception {

    final Server jetty = JettyHelper.initJetty("10.123.50.1", 10480, false, false, null, null, null);

    assertNotNull(jetty);
    assertNotNull(jetty.getConnectors()[0]);
    assertEquals(10480, ((ServerConnector) jetty.getConnectors()[0]).getPort());
  }

}
