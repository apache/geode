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
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.cache.operations.OperationContext.OperationCode;
import com.gemstone.gemfire.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests operation codes for DistributedSystemMXBean operations.
 */
@Category(UnitTest.class)
public class OperationCodesForDistributedSystemMXBeanTest {

  private final String[] distributedSystemMXBeanOperations = {
      "listCacheServerObjectNames",
      "viewRemoteClusterStatus",
      "getTotalHeapSize",
      "setQueryCollectionsDepth",
      "getQueryCollectionsDepth",
      "changeAlertLevel",
      "backupAllMembers",
      "revokeMissingDiskStores",
      "shutDownAllMembers",
      "queryData",
      "queryDataForCompressedResult",
      "setQueryResultSetLimit"
  };

  private final OperationCode[] distributedSystemResourceOperationCodes = {
      OperationCode.LIST_DS,
      OperationCode.LIST_DS,
      OperationCode.LIST_DS,
      OperationCode.QUERY,
      OperationCode.LIST_DS,
      OperationCode.CHANGE_ALERT_LEVEL,
      OperationCode.BACKUP_MEMBERS,
      OperationCode.REVOKE_MISSING_DISKSTORE,
      OperationCode.SHUTDOWN,
      OperationCode.QUERY,
      OperationCode.QUERY,
      OperationCode.QUERY
  };

  @Test
  public void operationsShouldMapToCodes() {
//    ObjectName objectName = MBeanJMXAdapter.getDistributedSystemName();
//    for (int i = 0; i < distributedSystemMXBeanOperations.length; i++) {
//      JMXOperationContext context = new JMXOperationContext(objectName, distributedSystemMXBeanOperations[i]);
//      assertEquals(distributedSystemResourceOperationCodes[i], context.getResourceOperationCode());
//      assertEquals(OperationCode.RESOURCE, context.getOperationCode());
//    }
  }
}
