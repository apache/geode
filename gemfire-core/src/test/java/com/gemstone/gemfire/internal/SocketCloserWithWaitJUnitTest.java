package com.gemstone.gemfire.internal;

import java.util.concurrent.TimeUnit;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Tests SocketCloser with a wait time. The default SocketCloser does not wait.
 * This test configures a closer much like the one used by CacheClientNotifier.
 */
@Category(UnitTest.class)
public class SocketCloserWithWaitJUnitTest extends SocketCloserJUnitTest {
  @Override
  protected SocketCloser createSocketCloser() {
    return new SocketCloser(
        SocketCloser.ASYNC_CLOSE_POOL_KEEP_ALIVE_SECONDS,
        1, // max threads
        1, TimeUnit.NANOSECONDS);
  }
}
