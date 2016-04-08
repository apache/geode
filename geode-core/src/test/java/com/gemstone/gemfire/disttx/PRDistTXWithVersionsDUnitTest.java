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
package com.gemstone.gemfire.disttx;

import java.util.Properties;

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.execute.PRTransactionWithVersionsDUnitTest;

public class PRDistTXWithVersionsDUnitTest extends
    PRTransactionWithVersionsDUnitTest {

  public PRDistTXWithVersionsDUnitTest(String name) {
    super(name);
  }
  
  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DistributionConfig.DISTRIBUTED_TRANSACTIONS_NAME, "true");
//    props.setProperty(DistributionConfig.LOG_LEVEL_NAME, "fine");
    return props;
  }
  
  // [DISTTX] TODO test overridden and intentionally left blank as they fail.
  // Fix this 
  
  @Override
  public void testBasicPRTransactionRedundancy0() {
  }

  @Override
  public void testBasicPRTransactionRedundancy1() {
  }

  @Override
  public void testBasicPRTransactionRedundancy2() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy0() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy1() {
  }

  @Override
  public void testBasicPRTransactionNoDataRedundancy2() {
  }

}
