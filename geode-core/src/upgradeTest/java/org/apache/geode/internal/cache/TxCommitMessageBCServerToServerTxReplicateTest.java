/**
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
package org.apache.geode.internal.cache;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import java.util.List;

import org.junit.Test;


public class TxCommitMessageBCServerToServerTxReplicateTest extends TxCommitMessageBCTestBase {

  @Test
  public void test() throws Exception {
    String regionName = REPLICATE_REGION_NAME;

    List<Integer> beforeValues =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionName));
    server1.invoke(() -> TxCommitMessageBCTestBase.doTxPuts(regionName));
    List<Integer> afterValues1 =
        server1.invoke(() -> TxCommitMessageBCTestBase.doGets(regionName));
    List<Integer> afterValues2 =
        server2.invoke(() -> TxCommitMessageBCTestBase.doGets(regionName));

    Integer expected1 = beforeValues.get(0) == null ? 1 : beforeValues.get(0) + 1;
    Integer expected2 = beforeValues.get(1) == null ? 1000 : beforeValues.get(1) + 1000;

    assertThat(afterValues1, contains(expected1, expected2));
    assertThat(afterValues2, contains(expected1, expected2));
  }

}
