package com.gemstone.gemfire.cache30;

/**
 * Same as BridgeMembershipDUnitTest but uses selector in server
 *
 * @author darrel
 * @since 5.1
 */
public class BridgeMembershipSelectorDUnitTest extends BridgeMembershipDUnitTest {
  public BridgeMembershipSelectorDUnitTest(String name) {
    super(name);
  }
  protected int getMaxThreads() {
    return 2;
  }
}
