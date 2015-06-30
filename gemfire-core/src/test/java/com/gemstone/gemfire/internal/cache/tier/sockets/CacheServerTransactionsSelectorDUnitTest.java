package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * Just like CacheServerTransactionsDUnitTest but configures bridge server
 * with thread pool (i.e. selector).
 * @author darrel
 */
public class CacheServerTransactionsSelectorDUnitTest
  extends CacheServerTransactionsDUnitTest
{
  /** constructor */
  public CacheServerTransactionsSelectorDUnitTest(String name) {
    super(name);
  }

  protected int getMaxThreads() {
    return 2;
  }
}
