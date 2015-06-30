package com.gemstone.gemfire.internal.cache.execute;

public class PRTransactionWithVersionsDUnitTest extends PRTransactionDUnitTest {

  public PRTransactionWithVersionsDUnitTest(String name) {
    super(name);
  }

  @Override
  protected boolean getEnableConcurrency() {
    return true;
  }
}
