package com.gemstone.gemfire.cache30;

/**
 * Same as BridgeWriterDUnitTest but uses selector in server
 *
 * @author darrel
 * @since 5.1
 */
public class BridgeWriterSelectorDUnitTest extends BridgeWriterDUnitTest {
  public BridgeWriterSelectorDUnitTest(String name) {
    super(name);
  }
  protected int getMaxThreads() {
    return 2;
  }
}
