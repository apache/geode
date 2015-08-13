package com.gemstone.gemfire.cache.query.dunit;

import java.security.Principal;

import junit.framework.TestCase;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheClosedException;
import com.gemstone.gemfire.cache.operations.ExecuteCQOperationContext;
import com.gemstone.gemfire.cache.operations.OperationContext;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.security.AccessControl;
import com.gemstone.gemfire.security.NotAuthorizedException;

public class CloseCacheAuthorization extends TestCase implements AccessControl {
  private DistributedMember remoteDistributedMember;
  private Cache cache;
  private LogWriter logger;

  public static AccessControl create() {
    return new CloseCacheAuthorization();
  }

  @Override
  public void close() {
  }

  @Override
  public void init(Principal principal, DistributedMember remoteMember,
      Cache cache) throws NotAuthorizedException {
    this.remoteDistributedMember = remoteMember;
    this.cache = cache;
    this.logger = cache.getSecurityLogger();
  }

  @Override
  public boolean authorizeOperation(String regionName, OperationContext context) {
    if (context instanceof ExecuteCQOperationContext) {
      cache.close();
      //return false;
      throw new CacheClosedException("cache is closed");
    }
    return true;
  }
}
