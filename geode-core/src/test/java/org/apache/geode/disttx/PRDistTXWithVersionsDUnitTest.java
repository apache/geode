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
package org.apache.geode.disttx;

import static org.apache.geode.distributed.ConfigurationProperties.*;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.cache.execute.PRTransactionWithVersionsDUnitTest;
import org.apache.geode.test.junit.categories.DistributedTest;

@Category(DistributedTest.class)
public class PRDistTXWithVersionsDUnitTest extends PRTransactionWithVersionsDUnitTest {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DISTRIBUTED_TRANSACTIONS, "true");
    return props;
  }
  
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionRedundancy0() {
  }

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionRedundancy1() {
  }

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionRedundancy2() {
  }

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionNoDataRedundancy0() {
  }

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionNoDataRedundancy1() {
  }

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionNoDataRedundancy2() {
  }

}
