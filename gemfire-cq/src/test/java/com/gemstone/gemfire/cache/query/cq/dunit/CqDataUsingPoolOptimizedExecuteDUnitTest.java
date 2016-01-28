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
package com.gemstone.gemfire.cache.query.cq.dunit;


import com.gemstone.gemfire.cache.query.internal.cq.CqServiceImpl;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;

/**
 * Test class for testing {@link CqServiceImpl#EXECUTE_QUERY_DURING_INIT} flag
 *
 */
public class CqDataUsingPoolOptimizedExecuteDUnitTest extends CqDataUsingPoolDUnitTest{

  public CqDataUsingPoolOptimizedExecuteDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    addExpectedException("Read timed out");
    addExpectedException("java.net.SocketException");
    super.setUp();
    invokeInEveryVM(new SerializableRunnable("set test hook") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = false;
      }
    });
  }
  
  @Override
  public void tearDown2() throws Exception {
    invokeInEveryVM(new SerializableRunnable("getSystem") {
      public void run() {
        CqServiceImpl.EXECUTE_QUERY_DURING_INIT = true;
      }
    });
    super.tearDown2();
  }
}
