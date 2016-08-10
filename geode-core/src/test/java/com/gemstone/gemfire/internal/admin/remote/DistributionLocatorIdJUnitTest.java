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
package com.gemstone.gemfire.internal.admin.remote;

import static com.gemstone.gemfire.internal.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;
import org.junit.Before; 
import org.junit.After;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/** 
* DistributionLocatorId Tester. 
*/
@Category(UnitTest.class)
public class DistributionLocatorIdJUnitTest {

  @Test
  public void testEquals() throws UnknownHostException {
    InetAddress address = InetAddress.getLocalHost();
    DistributionLocatorId dLI1 = new DistributionLocatorId(address, 40404, "127.0.0.1", null);
    DistributionLocatorId dLI2 = dLI1;
    @SuppressWarnings("RedundantStringConstructorCall")
    DistributionLocatorId dLI3 = new DistributionLocatorId(address, 40404, new String("127.0.0.1"), null);
    @SuppressWarnings("RedundantStringConstructorCall")
    DistributionLocatorId dLI4 = new DistributionLocatorId(InetAddress.getByName("localhost"), 50505, new String("128.0.0.1"), null);

    assertTrue(dLI1.equals(dLI2));
    assertTrue(dLI1.equals(dLI3));
    assertFalse(dLI1.equals(dLI4));

  }

} 
