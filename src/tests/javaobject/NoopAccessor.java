/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package javaobject;

import org.apache.geode.cache.Cache;
import org.apache.geode.security.AccessControl;
import org.apache.geode.cache.operations.OperationContext;
import org.apache.geode.distributed.DistributedMember;

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
