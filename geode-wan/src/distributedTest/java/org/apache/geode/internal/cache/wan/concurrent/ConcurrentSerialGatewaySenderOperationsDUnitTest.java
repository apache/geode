/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.wan.concurrent;

import java.io.IOException;

import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewaySender.OrderPolicy;
import org.apache.geode.internal.cache.wan.serial.SerialGatewaySenderOperationsDUnitTest;
import org.apache.geode.test.junit.categories.WanTest;

@Category(WanTest.class)
@SuppressWarnings("serial")
public class ConcurrentSerialGatewaySenderOperationsDUnitTest
    extends SerialGatewaySenderOperationsDUnitTest {

  @Override
  protected void createSenderInVm4() throws IOException {
    createSender("ln", 2, true, true, 5, OrderPolicy.KEY);
  }

  @Override
  protected void createSenderInVm5() throws IOException {
    createSender("ln", 2, true, true, 5, OrderPolicy.KEY);
  }
}
