package com.gemstone.gemfire.cache30;

/**
 * Same as parent but uses selector in server
 *
 * @author darrel
 * @since 5.1
 */
public class ClientRegisterInterestSelectorDUnitTest extends ClientRegisterInterestDUnitTest {
  public ClientRegisterInterestSelectorDUnitTest(String name) {
    super(name);
  }
  protected int getMaxThreads() {
    return 2;
  }
}
