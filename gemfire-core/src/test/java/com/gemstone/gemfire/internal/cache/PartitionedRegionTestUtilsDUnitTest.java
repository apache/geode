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

package com.gemstone.gemfire.internal.cache;

import hydra.GsRandom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Confirm that the utils used for testing work as advertised
 * @since 5.0 
 * @author mthomas
 *
 */
public class PartitionedRegionTestUtilsDUnitTest extends
    PartitionedRegionDUnitTestCase
{
  final int totalNumBuckets = 5;
  public PartitionedRegionTestUtilsDUnitTest(String name) {
    super(name);
  }

  /**
   * Test the {@link PartitionedRegion#getSomeKeys(Random)} method, making sure it 
   * returns keys when there are keys and {@link Collections#EMPTY_SET} when there are none.
   * @throws Exception
   */
  public void testGetKeys() throws Exception {
    final String r = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    CacheSerializableRunnable create = new CacheSerializableRunnable("CreatePartitionedRegion") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
          .setTotalNumBuckets(totalNumBuckets)
          .create());
        Region p = cache.createRegion(r, attr.create());
        assertNotNull(p);
        assertTrue(!p.isDestroyed());
        assertNull(p.get("Key"));
      }
    };

    vm0.invoke(create);
    vm1.invoke(create);
    vm2.invoke(create);
    
    vm0.invoke(new CacheSerializableRunnable("GetSomeKeys") {
      public void run2() throws CacheException {
        PartitionedRegion pr = (PartitionedRegion) getCache().getRegion(r);
        GsRandom rand = new GsRandom(123);
        // Assert that its empty
        for(int i=0; i<5; i++) {
          getLogWriter().info("Invocation " + i + " of getSomeKeys");
          try {
            Set s = null;
            s = pr.getSomeKeys(rand);
            assertNotNull(s);
            assertTrue(s.isEmpty());
          } catch (ClassNotFoundException cnfe) {
            fail("GetSomeKeys failed with ClassNotFoundException", cnfe);
          } catch (IOException ioe) {
            fail("GetSomeKeys failed with IOException", ioe);
          }
        }
        
        final int MAXKEYS=50;
        for(int i=0; i<MAXKEYS; i++) {
          pr.put("testKey" + i, new Integer(i));
        }
        
        // Assert not empty and has value in an accepable range
        for(int i=0; i<5; i++) {
          getLogWriter().info("Invocation " + i + " of getSomeKeys");
          try {
            Set s = null;
            s = pr.getSomeKeys(rand);
            assertNotNull(s);
            assertFalse(s.isEmpty());
            Integer val;
            getLogWriter().info("Invocation " + i + " got " + s.size() + " keys");
            for (Iterator it = s.iterator(); it.hasNext(); ) {
              Object key = it.next();
              getLogWriter().info("Key: " + key);
              val = (Integer) pr.get(key);
              assertNotNull(val);
              assertTrue(val.intValue() >= 0);
              assertTrue(val.intValue() < MAXKEYS); 
            }
          } catch (ClassNotFoundException cnfe) {
            fail("GetSomeKeys failed with ClassNotFoundException", cnfe);
          } catch (IOException ioe) {
            fail("GetSomeKeys failed with IOException", ioe);
          }
        }
      }
    });
  }
  
  /**
   * Test the test method PartitionedRegion.getAllNodes
   * Verify that it returns nodes after a value has been placed into the PartitionedRegion.
   * @see PartitionedRegion#getAllNodes()
   * 
   * @throws Exception
   */
  
  public static class TestGetNodesKey implements DataSerializable {
    int hc; 
    public TestGetNodesKey(int hc) { this.hc = hc; }
    public TestGetNodesKey() {};
    public int hashCode() {return this.hc; }
    public void toData(DataOutput out) throws IOException  {out.writeInt(this.hc); }
    public void fromData(DataInput in) throws IOException, ClassNotFoundException { this.hc = in.readInt(); } 
  }
  public void testGetNodes() throws Exception {
    final String r = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM validator = host.getVM(2);
    
    CacheSerializableRunnable createAndTest = new CacheSerializableRunnable("CreatePRAndTestGetAllNodes") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
          .setTotalNumBuckets(totalNumBuckets)
          .create());
        PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
        assertNotNull(p);
        assertTrue(!p.isDestroyed());

        // For each invocation, create a key that has a sequential hashCode.
        // Putting this key into the PR should force a new bucket allocation on 
        // each new VM (assuming a mod on the hashCode), forcing the number of VMs to increase
        // when we call getAllNodes each time this method is called.
        Integer i = (Integer) p.get("Counter");
        final Integer keyHash; 
        if (i == null) {
          i = new Integer(0);
        } else {
          i = new Integer(i.intValue() + 1);
        }
        keyHash = i;
        p.put("Counter", i);
        p.put(new TestGetNodesKey(keyHash.intValue()), i);
        Set allN = p.getAllNodes();
        assertNotNull(allN);
        assertTrue(! allN.isEmpty());
      }
    };

    
    validator.invoke(createAndTest);
    validator.invoke(new CacheSerializableRunnable("AssertGetNodesCreation1") {
      public void run2() throws CacheException
      {
        PartitionedRegion p = (PartitionedRegion) getCache().getRegion(r);
        assertNotNull(p);
        assertTrue(!p.isDestroyed());
        Set allN = p.getAllNodes();
        assertNotNull(allN);
        assertEquals(1, allN.size());
      }}
    );
    vm0.invoke(createAndTest);
    validator.invoke(new CacheSerializableRunnable("AssertGetNodesCreation2") {
      public void run2() throws CacheException
      {
        PartitionedRegion p = (PartitionedRegion) getCache().getRegion(r);
        assertNotNull(p);
        assertTrue(!p.isDestroyed());
        Set allN = p.getAllNodes();
        assertNotNull(allN);
        assertEquals(2, allN.size());
      }}
    );

    vm1.invoke(createAndTest);
    validator.invoke(new CacheSerializableRunnable("AssertGetNodesCreation3") {
      public void run2() throws CacheException
      {
        PartitionedRegion p = (PartitionedRegion) getCache().getRegion(r);
        assertNotNull(p);
        assertTrue(!p.isDestroyed());
        Set allN = p.getAllNodes();
        assertNotNull(allN);
        assertEquals(3, allN.size());
      }}
    );
  }

  /** 
   * Test the test utiltities that allow investigation of a PartitionedRegion's local cache. 
   * @throws Exception
   */
  public void testLocalCacheOps() throws Exception {
    final String r = getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
//    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    
    vm0.invoke(new CacheSerializableRunnable("CreatePR") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
              .setTotalNumBuckets(totalNumBuckets)
              .setLocalMaxMemory(8)
              .create());

        PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
        assertNotNull(p);
      }
    });
    
//  TODO enable this test when we have the LocalCache properly implemented -- mthomas 2/23/2006   
//    vm1.invoke(new CacheSerializableRunnable("CreatePRWithLocalCacheAndTestOps") {
//      public void run2() throws CacheException
//      {
//        Cache cache = getCache();
//        AttributesFactory attr = new AttributesFactory();
//        attr.setScope(Scope.DISTRIBUTED_ACK);
//        Properties lp = new Properties();
//        lp.setProperty(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY, "0");
//        attr.setPartitionAttributes(new PartitionAttributesFactory()
//              .setLocalProperties(lp)
//              .createPartitionAttributes());
//
//        PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
//        assertNotNull(p);
//        
//        final String key1 = "lcKey1"; final String val1 = "lcVal1";
//        final String key2 = "lcKey2"; final String val2 = "lcVal2";
//        // Test localCacheContainsKey
//        assertFalse(p.localCacheContainsKey(key1));
//        assertFalse(p.localCacheContainsKey(key2));
//        p.put(key1, val1);
//        assertFalse(p.localCacheContainsKey(key1));
//        assertFalse(p.localCacheContainsKey(key2));
//        assertEquals(val1, p.get(key1));
//        assertTrue(p.localCacheContainsKey(key1));
//        assertFalse(p.localCacheContainsKey(key2));
//
//        // test localCacheKeySet
//        Set lset = p.localCacheKeySet();
//        assertTrue(lset.contains(key1));
//        assertFalse(lset.contains(key2));
//        
//        // test localCacheGet
//        assertEquals(val1, p.localCacheGet(key1));
//        assertNull(p.localCacheGet(key2));
//        p.put(key2, val2);
//        assertNull(p.localCacheGet(key2));
//        assertEquals(val2, p.get(key2));
//        assertEquals(val2, p.localCacheGet(key2));
//      }
//    });

    vm2.invoke(new CacheSerializableRunnable("CreatePRWithNoLocalCacheAndTestOps") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
              .setTotalNumBuckets(totalNumBuckets)
              .setLocalMaxMemory(0)
              .create());

        PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
        assertNotNull(p);
        
        final String key3 = "lcKey3"; final String val3 = "lcVal3";
        final String key4 = "lcKey4"; final String val4 = "lcVal4";
        
        // Test localCacheContainsKey
        assertFalse(p.localCacheContainsKey(key3));
        assertFalse(p.localCacheContainsKey(key4));
        p.put(key3, val3);
        assertFalse(p.localCacheContainsKey(key3));
        assertFalse(p.localCacheContainsKey(key4));
        assertEquals(val3, p.get(key3));
        assertFalse(p.localCacheContainsKey(key3));
        assertFalse(p.localCacheContainsKey(key4));

        // test localCacheKeySet
        Set lset = p.localCacheKeySet();
        assertFalse(lset.contains(key3));
        assertFalse(lset.contains(key4));
        
        // test localCacheGet
        assertNull(val3, p.localCacheGet(key3));
        assertNull(p.localCacheGet(key4));
        p.put(key4, val4);
        assertNull(p.localCacheGet(key4));
        assertEquals(val4, p.get(key4));
        assertNull(p.localCacheGet(key4));
      }
    });
  }
  
  /**
   * Test the test method PartitionedRegion.getAllNodes
   * Verify that it returns nodes after a value has been placed into the PartitionedRegion.
   * @see PartitionedRegion#getAllNodes()
   * 
   * @throws Exception
   */
  public void testGetBucketKeys() throws Exception {
    final String r = getUniqueName();
    Host host = Host.getHost(0);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);
      
    CacheSerializableRunnable create = new CacheSerializableRunnable("CreatePR") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        attr.setPartitionAttributes(new PartitionAttributesFactory()
          .setTotalNumBuckets(totalNumBuckets)
          .create());
        PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
        assertNotNull(p);
      }
    };

    vm2.invoke(create);
    vm3.invoke(create);

    // Create an accessor
    Cache cache = getCache();
    AttributesFactory attr = new AttributesFactory();
        
    attr.setPartitionAttributes(new PartitionAttributesFactory()
        .setTotalNumBuckets(totalNumBuckets)
        .setLocalMaxMemory(0)
        .create());
    PartitionedRegion p = (PartitionedRegion) cache.createRegion(r, attr.create());
    assertNotNull(p);
    final int totalBucks = p.getTotalNumberOfBuckets();

    for (int i=totalBucks-1; i>=0; i--) {
      Set s = p.getBucketKeys(i);
      assertTrue(s.isEmpty());
    }

    class TestPRKey implements Serializable {
      int hashCode;
      int differentiator;
      TestPRKey(int hash, int differentiator) {
        this.hashCode = hash;
        this.differentiator = differentiator; 
      }
       public int hashCode() {return hashCode; }
       public boolean equals(Object obj) {
         if (! (obj instanceof TestPRKey)) {
           return false;
         }
         return ((TestPRKey) obj).differentiator == this.differentiator;
      }
      public String toString() { return "TestPRKey " + hashCode + " diff " + differentiator;  }
    }
    
    TestPRKey key;
    Integer val;
    
    // Create bucket number of keys, assuming a modulous per key hashCode
    // There should be one key per bucket
    p.put(new TestPRKey(0, 1), new Integer(0));
    p.put(new TestPRKey(0, 2), new Integer(1));
    p.put(new TestPRKey(0, 3), new Integer(2));
    Set s = p.getBucketKeys(0);
    assertEquals(3, s.size());
    assertEquals(0, ((TestPRKey) s.iterator().next()).hashCode());
    assertEquals(0, ((TestPRKey) s.iterator().next()).hashCode());
    assertEquals(0, ((TestPRKey) s.iterator().next()).hashCode());

    // Skip bucket zero since we have three keys there, but fill out all the rest with keys
    for (int i=totalBucks-1; i>0; i--) {
      key = new TestPRKey(i, 0);
      val = new Integer(i);
      p.put(key, val);
      // Integer gottenVal = (Integer) p.get(key);
      // assertEquals("Value for key: " + key + " val " + gottenVal + " wasn't expected " + val, val, gottenVal);
    }
    
    // Assert that the proper number of keys are placed in each bucket 
    for (int i=1; i<totalBucks; i++) {
      s = p.getBucketKeys(i);
      assertEquals(s.size(), 1);
      key = (TestPRKey) s.iterator().next();
      assertEquals(i, key.hashCode());
      // assertEquals(new Integer(i), p.get(key)); 
    }
  }
  
  /**
   * Test the test method {@link PartitionedRegion#getBucketOwnersForValidation(int)}
   * Verify that the information it discovers is the same as the local advisor.
   * @throws Exception
   */
  public void testGetBucketOwners() throws Exception {
    final String rName0 = getUniqueName() + "-r0";
    final String rName1 = getUniqueName() + "-r1";
    final String rName2 = getUniqueName() + "-r2";
    final String[] regions = {rName0, rName1, rName2};
    final int numBuckets = 3;
    final Host host = Host.getHost(0);
    final VM datastore1 = host.getVM(2);
    final VM datastore2 = host.getVM(3);
    final VM datastore3 = host.getVM(0);
    final VM accessor = host.getVM(1);
      
    final CacheSerializableRunnable create = new CacheSerializableRunnable("CreatePR") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory()
        .setTotalNumBuckets(numBuckets);

        for (int redundancy = 0; redundancy < regions.length; redundancy++) {
          paf.setRedundantCopies(redundancy);
          attr.setPartitionAttributes(paf.create());          
          PartitionedRegion p = 
            (PartitionedRegion) cache.createRegion(regions[redundancy], attr.create());
          assertNotNull(p);
          assertEquals(0, p.size());
        }
      }
    };

    datastore1.invoke(create);
    datastore2.invoke(create);
    datastore3.invoke(create);
    
    accessor.invoke(new CacheSerializableRunnable("CreateAccessorPR") {
      public void run2() throws CacheException
      {
        Cache cache = getCache();
        AttributesFactory attr = new AttributesFactory();
        PartitionAttributesFactory paf = new PartitionAttributesFactory()
         .setTotalNumBuckets(numBuckets)
         .setLocalMaxMemory(0);

        for (int redundancy = 0; redundancy < regions.length; redundancy++) {
          paf.setRedundantCopies(redundancy);
          attr.setPartitionAttributes(paf.create());          
          PartitionedRegion p = 
            (PartitionedRegion) cache.createRegion(regions[redundancy], attr.create());
          assertNotNull(p);
          assertEquals(0, p.size());
        }
      }
    });
    
    final CacheSerializableRunnable noBucketOwners = new CacheSerializableRunnable("AssertNoBucketOwners") {
      public void run2() throws CacheException
      {
        String[] regions = {rName0, rName1, rName2};
        for (int rs = 0; rs < regions.length; rs++) {
          PartitionedRegion p = (PartitionedRegion) getCache().getRegion(regions[rs]);
          assertNotNull(p);
          assertTrue(!p.isDestroyed());
          assertEquals(numBuckets, p.getTotalNumberOfBuckets());
          try {
            for(int i = 0; i < p.getTotalNumberOfBuckets(); i++) {
              assertEquals(0, p.getRegionAdvisor().getBucketOwners(i).size());
              assertEquals(0, p.getBucketOwnersForValidation(i).size());
            }
          } catch (ForceReattemptException noGood) {
            fail("Unexpected force retry", noGood);
          }
        }
      }
    };
    datastore1.invoke(noBucketOwners);
    datastore2.invoke(noBucketOwners);
    datastore3.invoke(noBucketOwners);
    accessor.invoke(noBucketOwners);
    
    accessor.invoke(new CacheSerializableRunnable("CreateOneBucket") {
      public void run2() throws CacheException
      {
        for (int rs = 0; rs < regions.length; rs++) {
          PartitionedRegion p = (PartitionedRegion) getCache().getRegion(regions[rs]);
          assertNotNull(p);
          assertEquals(3, p.getTotalNumberOfBuckets());
          // Create one bucket
          p.put(new Integer(0), "zero"); 
          assertEquals(1, p.getRegionAdvisor().getCreatedBucketsCount());
        }
      }
    });
    
    final CacheSerializableRunnable oneBucketOwner = new CacheSerializableRunnable("AssertSingleBucketPrimary") {
      public void run2() throws CacheException
      {
        for (int rs = 0; rs < regions.length; rs++) {
          PartitionedRegion p = (PartitionedRegion) getCache().getRegion(regions[rs]);
          try {
            for(Iterator it = p.getRegionAdvisor().getBucketSet().iterator(); it.hasNext(); ) {
              Integer bid = (Integer) it.next();
              assertEquals(p.getRedundantCopies() + 1, p.getRegionAdvisor().getBucketOwners(bid.intValue()).size());
              List prims = p.getBucketOwnersForValidation(bid.intValue());
              assertEquals(p.getRedundantCopies() + 1, prims.size());
              int primCount = 0;
              for (Iterator lit = prims.iterator(); lit.hasNext(); ) {
                Object[] memAndBoolean = (Object[]) lit.next();
                assertEquals(3, memAndBoolean.length); // memberId, isPrimary and hostToken(new)
                assertTrue(memAndBoolean[0] instanceof DistributedMember);
                assertEquals(Boolean.class, memAndBoolean[1].getClass());
                Boolean isPrimary = (Boolean) memAndBoolean[1];
                if (isPrimary.booleanValue()) {
                  primCount++;
                }
              }
              assertEquals(1, primCount);
            }
          } catch (ForceReattemptException noGood) {
            fail("Unexpected force retry", noGood);
          }
        }
      }
    };
    accessor.invoke(oneBucketOwner);
    datastore1.invoke(oneBucketOwner);
    datastore2.invoke(oneBucketOwner);
    datastore3.invoke(oneBucketOwner);

  }
}
