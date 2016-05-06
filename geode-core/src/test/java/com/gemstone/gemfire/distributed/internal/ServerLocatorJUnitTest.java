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
package com.gemstone.gemfire.distributed.internal;

import static org.junit.Assert.*;

import java.io.IOException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * The ServerLocatorJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * ServerLocator class.
 * </p>
 * TODO: write more unit tests for this class...
 * </p>
 * @see com.gemstone.gemfire.distributed.internal.ServerLocator
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
@Category(UnitTest.class)
public class ServerLocatorJUnitTest {

  @Test
  public void testProcessRequestProcessesLocatorStatusRequest() throws IOException {
    final ServerLocator serverLocator = createServerLocator();

    final Object response = serverLocator.processRequest(new LocatorStatusRequest());
    System.out.println("response="+response);
    assertTrue(response instanceof LocatorStatusResponse);
  }

  private ServerLocator createServerLocator() throws IOException {
    return new TestServerLocator();
  }

  private static class TestServerLocator extends ServerLocator {
    TestServerLocator() throws IOException {
      super();
    }
    @Override
    protected boolean readyToProcessRequests() {
      return true;
    }
    @Override
    LogWriterI18n getLogWriterI18n() {
      return new LocalLogWriter(InternalLogWriter.NONE_LEVEL);
    }
  }

}
