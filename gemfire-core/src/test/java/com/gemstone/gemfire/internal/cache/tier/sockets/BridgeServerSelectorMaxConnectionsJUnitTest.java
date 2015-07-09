package com.gemstone.gemfire.internal.cache.tier.sockets;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Just like its parent but enables the server thread pool (aka selector).
 * 
 * @author darrel
 * 
 */
@Category(IntegrationTest.class)
public class BridgeServerSelectorMaxConnectionsJUnitTest extends BridgeServerMaxConnectionsJUnitTest
{
  protected int getMaxThreads() {
    return 2; 
  }
}
