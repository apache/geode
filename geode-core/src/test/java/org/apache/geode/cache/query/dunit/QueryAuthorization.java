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
package com.gemstone.gemfire.cache.query.dunit;

import static org.junit.Assert.*;

import java.security.Principal;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.cache.operations.QueryOperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

/**
 * Test authorization class for testing the accessibility of query bind
 * parameters from QueryOperationContext
 * 
 * Used by test:
 *
 * <li/>QueryParamsAuthorizationDUnitTest (via reflection)
 */
public class QueryAuthorization implements AccessControl {

  private DistributedMember remoteDistributedMember;

  private LogWriter logger;

  public static AccessControl create() {
    return new QueryAuthorization();
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Principal principal, DistributedMember remoteMember, Cache cache) throws NotAuthorizedException {
    this.remoteDistributedMember = remoteMember;
    this.logger = cache.getSecurityLogger();
  }

  @Override
  public boolean authorizeOperation(String regionName, OperationContext context) {
    this.logger.info(" Authorizing request ");
    if (context instanceof QueryOperationContext) {
      Object[] params = ((QueryOperationContext) context).getQueryParams();
      assertTrue(" Bind parameters should not be null", params != null);
      assertEquals(" 2 bind parameters should have been received.", 2, params.length);
    }
    return true;
  }
}