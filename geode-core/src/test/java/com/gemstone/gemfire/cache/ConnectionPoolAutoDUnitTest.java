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
package com.gemstone.gemfire.cache;

import com.gemstone.gemfire.cache30.ClientServerTestCase;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import static org.junit.runners.MethodSorters.*;
import org.junit.FixMethodOrder;
import org.junit.experimental.categories.Category;

@FixMethodOrder(NAME_ASCENDING)
@Category(DistributedTest.class)
public class ConnectionPoolAutoDUnitTest extends ConnectionPoolDUnitTest {

  @Override
  protected final void postSetUpConnectionPoolDUnitTest() throws Exception {
    ClientServerTestCase.AUTO_LOAD_BALANCE = true;
    Invoke.invokeInEveryVM(new SerializableRunnable("setupAutoMode") {
      public void run() {
        ClientServerTestCase.AUTO_LOAD_BALANCE = true;
      }
    });
  }

  @Override
  protected final void postTearDownConnectionPoolDUnitTest() throws Exception {
    ClientServerTestCase.AUTO_LOAD_BALANCE  = false;
    Invoke.invokeInEveryVM(new SerializableRunnable("disableAutoMode") {
      public void run() {
        ClientServerTestCase.AUTO_LOAD_BALANCE = false;
      }
    });
  }
  
}
