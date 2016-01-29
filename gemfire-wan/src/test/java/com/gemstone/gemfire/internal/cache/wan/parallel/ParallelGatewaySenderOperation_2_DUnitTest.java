package com.gemstone.gemfire.internal.cache.wan.parallel;

import com.gemstone.gemfire.internal.cache.wan.concurrent.ConcurrentParallelGatewaySenderOperation_2_DUnitTest;
import com.gemstone.gemfire.test.dunit.VM;

public class ParallelGatewaySenderOperation_2_DUnitTest extends ConcurrentParallelGatewaySenderOperation_2_DUnitTest {

  private static final long serialVersionUID = 1L;

  public ParallelGatewaySenderOperation_2_DUnitTest(String name) {
    super(name);
  }

  protected void createSender(VM vm, int concurrencyLevel, boolean manualStart) {
    vm.invoke(() -> createSender("ln", 2, true, 100, 10, false, false, null, manualStart));
  }

  protected void createSenders(VM vm, int concurrencyLevel) {
    vm.invoke(() -> createSender("ln1", 2, true, 100, 10, false, false, null, true));
    vm.invoke(() -> createSender("ln2", 3, true, 100, 10, false, false, null, true));
  }
}
