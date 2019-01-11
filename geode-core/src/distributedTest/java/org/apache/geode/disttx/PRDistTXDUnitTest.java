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
package org.apache.geode.disttx;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_TRANSACTIONS;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.internal.cache.execute.PRTransactionDUnitTest;


public class PRDistTXDUnitTest extends PRTransactionDUnitTest {

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = super.getDistributedSystemProperties();
    props.setProperty(DISTRIBUTED_TRANSACTIONS, "true");
    return props;
  }

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as it does not apply to disttx.")
  @Test
  public void testTxWithNonColocatedGet() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as it does not apply to disttx.")
  @Test
  public void testTxWithNonColocatedOps() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as it does not apply to disttx.")
  @Test
  public void testTxWithOpsOnMovedBucket() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as it does not apply to disttx.")
  @Test
  public void testTxWithGetOnMovedBucketUsingBucketReadHook() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionRedundancy0() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionRedundancy1() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionRedundancy2() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionNoDataRedundancy0() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionNoDataRedundancy1() {}

  @Override
  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Test
  public void testBasicPRTransactionNoDataRedundancy2() {}

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testBasicPRTransactionNonColocatedFunction0() {}

  @Ignore("[DISTTX] TODO test overridden and intentionally left blank as they fail.")
  @Override
  @Test
  public void testCommitToFailAfterPrimaryBucketMoved() {}
}
