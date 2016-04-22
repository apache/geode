/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
