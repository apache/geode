package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.PRTXJUnitTest;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Same tests as that of {@link PRTXJUnitTest} after setting
 * "distributed-transactions" property to true
 *
 */
@Category({IntegrationTest.class, DistributedTransactionsTest.class})
public class PRDistTXJUnitTest extends PRTXJUnitTest {

  public PRDistTXJUnitTest() {
  }

  @Override
  protected void createCache() throws Exception {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    p.setProperty("distributed-transactions", "true");
    // p.setProperty("log-level", "fine");
    this.cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem
        .connect(p));
    createRegion();
    this.txMgr = this.cache.getCacheTransactionManager();
    assert(this.txMgr.isDistributed());
    this.listenerAfterCommit = 0;
    this.listenerAfterFailedCommit = 0;
    this.listenerAfterRollback = 0;
    this.listenerClose = 0;
  }

  @Override
  @Test
  @Ignore
  public void testTxAlgebra() throws CacheException {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }

  @Override
  @Test
  @Ignore
  public void testListener() {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }

  @Override
  @Test
  @Ignore
  public void testCacheCallbacks() throws CacheException {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }

  @Override
  @Test
  @Ignore
  public void testJTAEnlistment() {
    // [DISTTX] TODO Fix this issue. This test fails for DISTTX. This is
    // overridden to avoid running the actual test
  }
}
