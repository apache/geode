/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package javaobject;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;

import java.security.Principal;

public class NoopAccessor implements AccessControl {

  public static AccessControl create() {
    return new NoopAccessor();
  }

  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) {
  }

  public boolean authorizeOperation(String regionName,
                                    OperationContext context) {
    return true;
  }

  public void close() {
  }
}
