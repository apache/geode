/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.dunit;

import java.security.Principal;

import junit.framework.TestCase;

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
 * @author tnomulwar
 * 
 */
public class QueryAuthorization extends TestCase implements AccessControl {
  private DistributedMember remoteDistributedMember;

  private LogWriter logger;

  public static AccessControl create() {
    return new QueryAuthorization();
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {
    this.remoteDistributedMember = remoteMember;
    this.logger = cache.getSecurityLogger();
  }

  @Override
  public boolean authorizeOperation(String regionName, OperationContext context) {
    this.logger.info(" Authorizing request ");
    if (context instanceof QueryOperationContext) {
      Object[] params = ((QueryOperationContext) context).getQueryParams();
      assertTrue(" Bind parameters should not be null", params != null);
      assertEquals(" 2 bind parameters should have been received.", 2,
          params.length);
    }
    return true;
  }

}