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
package com.gemstone.gemfire.test.dunit.tests;

import static org.assertj.core.api.Assertions.*;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

@SuppressWarnings("serial")
@Category(DistributedTest.class)
public class GetTestMethodNameDUnitTest extends DistributedTestCase {

  public GetTestMethodNameDUnitTest(final String name) {
    super(name);
  }

  public void testGetTestMethodName() {
    assertGetTestMethodName("testGetTestMethodName");
  }
  
  public void testGetTestMethodNameChanges() {
    assertGetTestMethodName("testGetTestMethodNameChanges");
  }
  
  public void testGetTestMethodNameInAllVMs() {
    assertGetTestMethodName("testGetTestMethodNameInAllVMs");
    
    for (int vmIndex = 0; vmIndex < Host.getHost(0).getVMCount(); vmIndex++) {
      Host.getHost(0).getVM(vmIndex).invoke(()->assertGetTestMethodName("testGetTestMethodNameInAllVMs"));
    }
  }
  
  private void assertGetTestMethodName(final String expected) {
    assertThat(getTestMethodName()).isEqualTo(expected);
  }
}
