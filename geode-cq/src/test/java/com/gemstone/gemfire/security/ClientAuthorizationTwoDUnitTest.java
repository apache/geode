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
package com.gemstone.gemfire.security;

import static com.gemstone.gemfire.security.SecurityTestUtils.*;
import static com.gemstone.gemfire.test.dunit.IgnoredException.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

/**
 * Tests for authorization from client to server. This tests for authorization
 * of all operations with both valid and invalid credentials/modules with
 * pre-operation callbacks. It also checks for authorization in case of
 * failover.
 * 
 * This is the second part of the test which had become long enough to
 * occasionally go beyond the 10min limit.
 * 
 * @since GemFire 5.5
 */
@Category({ DistributedTest.class, SecurityTest.class })
public class ClientAuthorizationTwoDUnitTest extends ClientAuthorizationTestCase {

  @Override
  public final void postSetUpClientAuthorizationTestBase() throws Exception {
    addIgnoredException("Read timed out");
    addIgnoredException("Connection reset");
    addIgnoredException("SocketTimeoutException");
    addIgnoredException("ServerConnectivityException");
    addIgnoredException("Socket Closed");
  }

  @Override
  public final void preTearDownClientAuthorizationTestBase() throws Exception {
    closeCache();
  }

  @Test
  public void testAllOpsWithFailover2() throws Exception {
    runOpsWithFailOver(allOps(), "testAllOpsWithFailover2");
  }

  private OperationWithAction[] allOps() {
    return new OperationWithAction[] {
        // Register interest in all KEYS using list
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3, OpFlags.USE_LIST | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 1, OpFlags.USE_LIST, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT, 2),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using list
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 1, OpFlags.USE_OLDCONN | OpFlags.USE_LIST, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all KEYS using regular expression
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3, OpFlags.USE_REGEX | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2, OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using regular expression
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2, OpFlags.USE_OLDCONN | OpFlags.USE_REGEX, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register interest in all KEYS using ALL_KEYS
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 3, OpFlags.USE_ALL_KEYS | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.REGISTER_INTEREST, 2, OpFlags.USE_ALL_KEYS, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Unregister interest in all KEYS using ALL_KEYS
        new OperationWithAction(OperationCode.UNREGISTER_INTEREST, 2, OpFlags.USE_OLDCONN | OpFlags.USE_ALL_KEYS, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Register CQ
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3, OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_NEWVAL, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Stop CQ
        new OperationWithAction(OperationCode.STOP_CQ, 3, OpFlags.USE_OLDCONN | OpFlags.CHECK_EXCEPTION, 4),
        new OperationWithAction(OperationCode.STOP_CQ, 1, OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL | OpFlags.LOCAL_OP, 4),

        // Restart the CQ
        new OperationWithAction(OperationCode.EXECUTE_CQ, 3, OpFlags.USE_NEWVAL | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        // UPDATE and test with GET
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_OLDCONN | OpFlags.LOCAL_OP, 4),

        // Close CQ
        new OperationWithAction(OperationCode.CLOSE_CQ, 3, OpFlags.USE_OLDCONN, 4),
        new OperationWithAction(OperationCode.CLOSE_CQ, 1, OpFlags.USE_OLDCONN, 4),
        // UPDATE and test with GET for no updates
        new OperationWithAction(OperationCode.PUT, 2, OpFlags.USE_OLDCONN | OpFlags.USE_NEWVAL, 4),
        new OperationWithAction(OperationCode.EXECUTE_CQ, 1, OpFlags.USE_OLDCONN | OpFlags.CHECK_FAIL | OpFlags.LOCAL_OP, 4),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CLEAR and check with GET
        new OperationWithAction(OperationCode.REGION_CLEAR, 3, OpFlags.CHECK_NOTAUTHZ, 1),
        new OperationWithAction(OperationCode.REGION_CLEAR, 1, OpFlags.NONE, 1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY | OpFlags.CHECK_FAIL, 8),
        // Repopulate the region
        new OperationWithAction(OperationCode.PUT),

        OperationWithAction.OPBLOCK_END,

        // Do REGION_CREATE and check with CREATE/GET
        new OperationWithAction(OperationCode.REGION_CREATE, 3, OpFlags.ENABLE_DRF | OpFlags.CHECK_NOTAUTHZ, 1),
        new OperationWithAction(OperationCode.REGION_CREATE, 1, OpFlags.ENABLE_DRF, 1),
        new OperationWithAction(OperationCode.PUT, 3, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION | OpFlags.CHECK_NOTAUTHZ, 4),
        new OperationWithAction(OperationCode.PUT, 1, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION, 4),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY | OpFlags.USE_SUBREGION, 4),

        // Do REGION_DESTROY of the sub-region and check with GET
        new OperationWithAction(OperationCode.REGION_DESTROY, 3, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION | OpFlags.NO_CREATE_SUBREGION | OpFlags.CHECK_NOTAUTHZ, 1),
        new OperationWithAction(OperationCode.REGION_DESTROY, 1, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION | OpFlags.NO_CREATE_SUBREGION, 1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.USE_OLDCONN | OpFlags.USE_SUBREGION | OpFlags.CHECK_EXCEPTION, 4),

        // Do REGION_DESTROY of the region and check with GET
        new OperationWithAction(OperationCode.REGION_DESTROY, 3, OpFlags.CHECK_NOTAUTHZ, 1),
        new OperationWithAction(OperationCode.REGION_DESTROY, 1, OpFlags.NONE, 1),
        new OperationWithAction(OperationCode.GET, 2, OpFlags.CHECK_NOKEY | OpFlags.CHECK_EXCEPTION, 4),

        // Skip failover for region destroy since it shall fail without restarting the server
        OperationWithAction.OPBLOCK_NO_FAILOVER };
  }
}
