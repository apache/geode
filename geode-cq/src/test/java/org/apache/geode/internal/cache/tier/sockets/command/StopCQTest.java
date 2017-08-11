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

package org.apache.geode.internal.cache.tier.sockets.command;

import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;
import org.apache.geode.security.ResourcePermission.Target;
import org.apache.geode.test.dunit.rules.CQUnitTestRule;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class StopCQTest {

  @Rule
  public CQUnitTestRule cqRule = new CQUnitTestRule();

  @Test
  public void needClusterManageQueryToStopCQ() throws Exception {
    StopCQ stopCQ = mock(StopCQ.class);
    doCallRealMethod().when(stopCQ).cmdExecute(cqRule.message, cqRule.connection,
        cqRule.securityService, 0);

    stopCQ.cmdExecute(cqRule.message, cqRule.connection, cqRule.securityService, 0);

    verify(cqRule.securityService).authorize(Resource.CLUSTER, Operation.MANAGE, Target.QUERY);
  }
}
