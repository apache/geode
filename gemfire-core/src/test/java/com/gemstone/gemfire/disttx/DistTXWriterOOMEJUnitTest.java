package com.gemstone.gemfire.disttx;

import java.util.Properties;

import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.TXWriterOOMEJUnitTest;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.test.junit.categories.DistributedTransactionsTest;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Same tests as that of {@link TXWriterOOMEJUnitTest} after setting
 * "distributed-transactions" property to true
 */
@Category({ IntegrationTest.class, DistributedTransactionsTest.class })
public class DistTXWriterOOMEJUnitTest extends TXWriterOOMEJUnitTest {

  public DistTXWriterOOMEJUnitTest() {
  }

  protected void createCache() throws CacheException {
    Properties p = new Properties();
    p.setProperty("mcast-port", "0"); // loner
    p.setProperty("distributed-transactions", "true");
    this.cache = (GemFireCacheImpl) CacheFactory.create(DistributedSystem
        .connect(p));
    AttributesFactory<?, ?> af = new AttributesFactory<String, String>();
    af.setScope(Scope.DISTRIBUTED_NO_ACK);
    af.setIndexMaintenanceSynchronous(true);
    this.region = this.cache.createRegion("TXTest", af.create());
    this.txMgr = this.cache.getCacheTransactionManager();
    assert (this.txMgr.isDistributed());
  }

}
