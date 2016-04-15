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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.Map;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.query.Aggregator;
import com.gemstone.gemfire.cache.query.CacheUtils;
import com.gemstone.gemfire.cache.query.IndexExistsException;
import com.gemstone.gemfire.cache.query.IndexNameConflictException;
import com.gemstone.gemfire.cache.query.NameResolutionException;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.RegionNotFoundException;
import com.gemstone.gemfire.cache.query.data.Portfolio;
import com.gemstone.gemfire.cache.query.functional.UDATestImpl;
import com.gemstone.gemfire.cache.query.functional.UDATestInterface;
import com.gemstone.gemfire.cache.query.functional.UDATestImpl.SumUDA;
import com.gemstone.gemfire.cache.query.internal.aggregate.uda.UDAManager;
import com.gemstone.gemfire.cache30.CacheTestCase;
import com.gemstone.gemfire.cache30.CacheXmlGeode10DUnitTest.UDACLass1;
import com.gemstone.gemfire.cache30.CacheXmlGeode10DUnitTest.UDACLass2;
import com.gemstone.gemfire.cache30.CacheXmlGeode10DUnitTest.UDACLass3;
import com.gemstone.gemfire.cache30.CacheXmlTestCase;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXml;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.ClientCacheCreation;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;

import com.gemstone.gemfire.test.dunit.AsyncInvocation;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

@Category(DistributedTest.class)
public class UDACreationDUnitTest extends CacheTestCase {
  public UDACreationDUnitTest(String name) {
    super(name);
  }

  @Test
  public void testUDACreationThroughProfileExchange() throws Exception {
    Host host = Host.getHost(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = getCache();
    final String udaName = "uda1";
    QueryService qs = CacheUtils.getQueryService();
    qs.createUDA(udaName, "com.gemstone.gemfire.cache.query.dunit.UDACreationDUnitTest$SumUDA");
    createCache(vm1, vm2, vm3);
    validateUDAExists(udaName, vm1, vm2, vm3);
    this.closeCache(vm1, vm2, vm3);
  }

  @Test
  public void testUDACreationThroughMessage() throws Exception {
    Host host = Host.getHost(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = getCache();
    createCache(vm1, vm2, vm3);
    final String udaName = "uda1";
    QueryService qs = CacheUtils.getQueryService();
    qs.createUDA(udaName, "com.gemstone.gemfire.cache.query.dunit.UDACreationDUnitTest$SumUDA");
    validateUDAExists(udaName, vm1, vm2, vm3);
    this.closeCache(vm1, vm2, vm3);
  }

  @Test
  public void testUDARemovalThroughMessage() throws Exception {
    Host host = Host.getHost(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    Cache cache = getCache();
    createCache(vm1, vm2, vm3);
    final String udaName = "uda1";
    QueryService qs = CacheUtils.getQueryService();
    qs.createUDA(udaName, "com.gemstone.gemfire.cache.query.dunit.UDACreationDUnitTest$SumUDA");
    validateUDAExists(udaName, vm1, vm2, vm3);
    qs.removeUDA(udaName);
    validateUDADoesNotExists(udaName, vm1, vm2, vm3);
    this.closeCache(vm1, vm2, vm3);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testUDAProfileMerge() {
    Host host = Host.getHost(0);
    final VM vm1 = host.getVM(1);

    final CacheCreation cacheCreation1 = new CacheCreation();
    cacheCreation1.addUDA("uda1", UDACLass1.class.getName());
    Helper helper = new Helper();
    helper.createCacheThruXML(cacheCreation1);
    final Cache c = getCache();
    assertNotNull(c);
    UDAManager udaMgr = ((GemFireCacheImpl) c).getUDAManager();
    Map<String, String> map = udaMgr.getUDAs();
    assertEquals(map.get("uda1"), UDACLass1.class.getName());

    // Now in VM1 create another cache through XMl containing uda2
    vm1.invoke(new SerializableRunnable("Create Cache in other VM") {
      public void run() {

        final CacheCreation cacheCreation2 = new CacheCreation();
        cacheCreation2.addUDA("uda2", UDACLass2.class.getName());
        cacheCreation2.addUDA("uda3", UDACLass3.class.getName());
        Helper helper = new Helper();
        helper.createCacheThruXML(cacheCreation2);
        final Cache c = getCache();

      }
    });
    // This VM should also have 3 UDAs at the end of intialization of remote vm

    map = udaMgr.getUDAs();
    assertEquals(map.get("uda1"), UDACLass1.class.getName());
    assertEquals(map.get("uda2"), UDACLass2.class.getName());
    assertEquals(map.get("uda3"), UDACLass3.class.getName());

    vm1.invoke(new SerializableRunnable("Create Cache in other VM") {
      public void run() {
        // validate at the end of intialization, there exists 3 UDAs
        UDAManager udaMgr = ((GemFireCacheImpl) getCache()).getUDAManager();
        Map<String, String> map = udaMgr.getUDAs();
        assertEquals(map.get("uda1"), UDACLass1.class.getName());
        assertEquals(map.get("uda2"), UDACLass2.class.getName());
        assertEquals(map.get("uda3"), UDACLass3.class.getName());

      }
    });

  }
  
  
  @Test
  @SuppressWarnings("rawtypes")
  public void testUDAProfileMergeMultipleVMs() throws Exception {
    Host host = Host.getHost(0);
    final VM vm0 = host.getVM(0);
    final VM vm1 = host.getVM(1);
    final VM vm2 = host.getVM(2);
    final VM vm3 = host.getVM(3);
    AsyncInvocation one = this.createCacheWithUDAAsynchronously("uda1", UDACLass1.class.getName(), vm1);
    AsyncInvocation two = this.createCacheWithUDAAsynchronously("uda2", UDACLass2.class.getName(), vm2);
    AsyncInvocation three = this.createCacheWithUDAAsynchronously("uda3", UDACLass3.class.getName(), vm3);
   
    final Cache c = getCache();
    assertNotNull(c);
    one.join();
    two.join();
    three.join();
    UDAManager udaMgr = ((GemFireCacheImpl) c).getUDAManager();
    Map<String, String> map = udaMgr.getUDAs();   
    assertEquals(map.get("uda1"), UDACLass1.class.getName());
    assertEquals(map.get("uda2"), UDACLass2.class.getName());
    assertEquals(map.get("uda3"), UDACLass3.class.getName());

    validateUDAExists("uda1", vm1, vm2, vm3);
    validateUDAExists("uda2", vm1, vm2, vm3);
    validateUDAExists("uda3", vm1, vm2, vm3);

  }

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void postTearDownCacheTestCase() throws Exception {
    disconnectAllFromDS();
  }

  private void createCache(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable("create cache") {
        public void run() {
          Cache cache = getCache();
        }
      });
    }
  }

  private void validateUDAExists(final String udaName, VM... vms) {
    try {
      Class<Aggregator> udaClass = ((GemFireCacheImpl) this.getCache()).getUDAManager().getUDAClass(udaName);
      assertNotNull(udaClass);
    } catch (NameResolutionException nre) {
      throw new RuntimeException(nre);
    }
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable("validate UDA exists") {
        public void run() {
          try {
            Class<Aggregator> udaClass = ((GemFireCacheImpl) getCache()).getUDAManager().getUDAClass(udaName);
            assertNotNull(udaClass);
          } catch (NameResolutionException nre) {
            throw new RuntimeException(nre);
          }

        }
      });
    }
  }
  
  private AsyncInvocation createCacheWithUDAAsynchronously(final String udaName, 
      final String udaClass, VM vm) {
    // Now in VM1 create another cache through XMl containing uda2
    return vm.invokeAsync(new SerializableRunnable("Create Cache in other VM") {
      public void run() {
        final CacheCreation cacheCreation2 = new CacheCreation();
        cacheCreation2.addUDA(udaName, udaClass);       
        Helper helper = new Helper();
        helper.createCacheThruXML(cacheCreation2);
        final Cache c = getCache();

      }
    });
  }

  private void validateUDADoesNotExists(final String udaName, VM... vms) {
    try {
      Class<Aggregator> udaClass = ((GemFireCacheImpl) this.getCache()).getUDAManager().getUDAClass(udaName);
      fail("UDA should not exist");
    } catch (NameResolutionException nre) {
      // OK
    }
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable("validate UDA exists") {
        public void run() {
          try {
            Class<Aggregator> udaClass = ((GemFireCacheImpl) getCache()).getUDAManager().getUDAClass(udaName);
            fail("UDA should not exist");
          } catch (NameResolutionException nre) {
            // OK
          }

        }
      });
    }
  }

  private void closeCache(VM... vms) {
    for (VM vm : vms) {
      vm.invoke(new SerializableRunnable() {
        public void run() {
          getCache().close();
        }
      });
    }
  }

  protected String getGemFireVersion() {
    return CacheXml.VERSION_1_0;
  }

  public static class SumUDA implements Aggregator {

    @Override
    public void accumulate(Object value) {

    }

    @Override
    public void init() {

    }

    @Override
    public Object terminate() {
      return null;
    }

    @Override
    public void merge(Aggregator otherAggregator) {

    }

  }

  public class UDACLass1 implements Aggregator {

    @Override
    public void accumulate(Object value) {
    }

    @Override
    public void init() {
    }

    @Override
    public Object terminate() {
      return null;
    }

    @Override
    public void merge(Aggregator otherAggregator) {
    }

  }

  public class UDACLass2 implements Aggregator {

    @Override
    public void accumulate(Object value) {
    }

    @Override
    public void init() {
    }

    @Override
    public Object terminate() {
      return null;
    }

    @Override
    public void merge(Aggregator otherAggregator) {
    }

  }

  public class UDACLass3 implements Aggregator {

    @Override
    public void accumulate(Object value) {
    }

    @Override
    public void init() {
    }

    @Override
    public Object terminate() {
      return null;
    }

    @Override
    public void merge(Aggregator otherAggregator) {
    }

  }

  public static class Helper extends CacheXmlTestCase {
    public Helper() {
      super("UDACreationDUnitTest:testUDAProfileMerge");
      CacheXmlTestCase.lonerDistributedSystem = false;
    }

    public void createCacheThruXML(CacheCreation creation) {
      this.testXml(creation, true);
    }

    @Override
    protected String getGemFireVersion() {
      return CacheXml.VERSION_1_0;
    }

    @Override
    protected boolean getUseSchema() {
      return true;
    }
    
    @Test
    public void testDummy(){}

  }

}
