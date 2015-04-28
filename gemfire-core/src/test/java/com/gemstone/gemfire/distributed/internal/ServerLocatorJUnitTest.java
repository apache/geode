/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import static org.junit.Assert.*;

import java.io.IOException;

import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusRequest;
import com.gemstone.gemfire.cache.client.internal.locator.LocatorStatusResponse;
import com.gemstone.gemfire.i18n.LogWriterI18n;
import com.gemstone.gemfire.internal.logging.LocalLogWriter;
import com.gemstone.gemfire.internal.logging.InternalLogWriter;
import com.gemstone.junit.UnitTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The ServerLocatorJUnitTest class is a test suite of test cases testing the contract and functionality of the
 * ServerLocator class.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.internal.ServerLocator
 * @see org.junit.Assert
 * @see org.junit.Test
 * @since 7.0
 */
// TODO Dan, write more unit tests for this class...
@Category(UnitTest.class)
public class ServerLocatorJUnitTest {

  protected ServerLocator createServerLocator() throws IOException {
    return new TestServerLocator();
  }

  @Test
  public void testProcessRequestProcessesLocatorStatusRequest() throws IOException {
    final ServerLocator serverLocator = createServerLocator();

    final Object response = serverLocator.processRequest(new LocatorStatusRequest());
    System.out.println("response="+response);
    assertTrue(response instanceof LocatorStatusResponse);
  }

  protected static class TestServerLocator extends ServerLocator {
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
