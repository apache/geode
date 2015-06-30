package com.gemstone.gemfire.internal.cache.tier.sockets;

/**
 * Just like InterestListEndpointDUnitTest but uses thread pool (i.e. selector)
 * in bridge servers
 * @author darrel
 *
 */
public class InterestListEndpointSelectorDUnitTest
  extends InterestListEndpointDUnitTest
{
  public InterestListEndpointSelectorDUnitTest(String name) {
    super(name);
  }

  protected int getMaxThreads() {
    return 2;
  }
}
