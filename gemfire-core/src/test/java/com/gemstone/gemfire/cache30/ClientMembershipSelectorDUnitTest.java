package com.gemstone.gemfire.cache30;

/**
 * Same as parent but uses selector in server
 *
 * @author darrel
 * @since 5.1
 */
public class ClientMembershipSelectorDUnitTest extends ClientMembershipDUnitTest {
  public ClientMembershipSelectorDUnitTest(String name) {
    super(name);
  }
  protected int getMaxThreads() {
    return 2;
  }
}
