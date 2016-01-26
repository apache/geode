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
package com.gemstone.gemfire.cache30;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;

import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.EntryNotFoundException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * An instance of this class is delegated to by test classes that test
 * disk regions.
 *
 * @author Eric Zoerner
 *
 */
public class DiskRegionTestImpl implements Serializable {
  
  final RegionTestCase rtc;

  private CacheSerializableRunnable createRgnRunnable(final String name) {
    return new CacheSerializableRunnable("Create region") {
      public void run2() throws CacheException {
        DiskRegionTestImpl.this.rtc.createRegion(name);
      }
    };
  }

  /** Creates a new instance of DiskRegionTestImpl */
  public DiskRegionTestImpl(RegionTestCase rtc) {
    this.rtc = rtc;
  }
  
  /**
   * Tests that you can create a disk region
   */
  public void testCreateDiskRegion() throws CacheException {
    final String name = this.rtc.getUniqueName();
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    
    vm0.invoke(createRgnRunnable(name));
  }
  
  
  
  
  private static final int NUM_ENTRIES = 1000;
  private static final int VALUE_SIZE = 2000;

  /**
   * Tests fillValues on backup regions.
   *
   * Note: The regions in the following description all have the same unique name.
   * 1) Create backup region in VM0 and add some values so they get backed up
   * 2) Close that region
   * 3) Create non-mirrored distributed region in VM1 and populate with
   *    over 1M of data
   * 4) Create a mirrored KEYS region in VM2. This will cause VM2 to have all
   *    the keys but no values.
   * 5) Re-create the backup region in VM0 with mirroring KEY_VALUES.
   *    This will get the keys from VM2 and the values from VM1 using fillValues.
   *    The region should end up with the keys created in step 1, and they should
   *    not be faulted into the VM.
   */
  public void testBackupFillValues() throws CacheException {
    RegionAttributes attrs = this.rtc.getRegionAttributes();
    assertTrue("This test not appropriate for non-backup regions",
                attrs.getPersistBackup());
    
    final String name = this.rtc.getUniqueName();
    final String key1 = "KEY1";
    final String key2 = "KEY2";
    final String value1 = "VALUE1";
    final String value2 = "VALUE2";
    
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
//    VM vm2 = host.getVM(2);
    
    vm0.invoke(new CacheSerializableRunnable("Create backup Region in VM0") {
      public void run2() throws CacheException {
        Region rgn = DiskRegionTestImpl.this.rtc.createRegion(name);
        rgn.create(key1, value1);
        rgn.create(key2, value2);
        
        // create entries that will be overwritten by getInitialImage below
        rgn.create(new Integer(0), "TEMP-0");
        rgn.create(new Integer(1), "TEMP-1");
        
        // no longer to close cache in 6.5, otherwise the 2 vms will splitbrain
        // CacheTestCase.closeCache();
      }
    });
    
    vm1.invoke(new CacheSerializableRunnable("Create & Populate non-mirrored in VM1") {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory();
        // set scope to be same as test region
        Scope scope = DiskRegionTestImpl.this.rtc.getRegionAttributes().getScope();
        factory.setScope(scope);
        DataPolicy dataPolicy = DiskRegionTestImpl.this.rtc.getRegionAttributes().getDataPolicy();
        factory.setDataPolicy(dataPolicy);
        RegionAttributes attrs2 = factory.create();
        Region rgn = DiskRegionTestImpl.this.rtc.createRegion(name, attrs2);
        
        // Fill the region with some keys.
        for (int i = 0; i < NUM_ENTRIES; i++) {
          byte[] value = new byte[VALUE_SIZE];
          Arrays.fill(value, (byte)0xAB);
          rgn.put(new Integer(i), value);
        }
        // just for sanity:
        assertEquals(NUM_ENTRIES+2, rgn.keySet().size());
        }
    });

    vm0.invoke(new CacheSerializableRunnable("Close Cache in VM0") {
      public void run2() throws CacheException {
         CacheTestCase.closeCache();
      }
    });

//     vm2.invoke(new CacheSerializableRunnable("Create mirrored KEYS region in VM2") {
//       public void run2() throws CacheException {
//         AttributesFactory factory = new AttributesFactory();
//         // set scope to be same as test region
//         Scope scope = DiskRegionTestImpl.this.rtc.getRegionAttributes().getScope();
//         factory.setScope(scope);
//         // set mirror KEYS
//         factory.setMirrorType(MirrorType.KEYS);
//         RegionAttributes attrs2 = factory.create();
//         Region rgn = DiskRegionTestImpl.this.rtc.createRegion(name, attrs2);
//         }
//     });
    
    String runnableName = "Re-create backup region in VM0 with mirror " +
                          "KEYS_VALUES and Do Verification";
    vm0.invoke(new CacheSerializableRunnable(runnableName) {
      public void run2() throws CacheException {
        AttributesFactory factory = new AttributesFactory(
          DiskRegionTestImpl.this.rtc.getRegionAttributes());
        factory.setDataPolicy(DataPolicy.PERSISTENT_REPLICATE);
        RegionAttributes attrs2 = factory.create();
//        DebuggerSupport.waitForJavaDebugger(rtc.getLogWriter(), "About to create region...");
        Region rgn = DiskRegionTestImpl.this.rtc.createRegion(name, attrs2);
        
        // verify
        assertEquals(NUM_ENTRIES + 2, rgn.keySet().size());

        boolean RECOVER_VALUES = true;

        if (RECOVER_VALUES) {
          assertEquals(value1, rgn.getEntry(key1).getValue());
          assertEquals(value2, rgn.getEntry(key2).getValue());
        } else {
          assertNull(valueInVM(rgn, key1));
          assertNull(valueInVM(rgn, key2));
        }
        assertEquals(value1, valueOnDisk(rgn, key1));
        assertEquals(value2, valueOnDisk(rgn, key2));
        
        // The following also verifies TEMP values were overwritten
        for (int i = 0; i < NUM_ENTRIES; i++) {
          Region.Entry entry = rgn.getEntry(new Integer(i));
          assertNotNull("No entry for key " + i, entry);
          byte[] v = (byte[])entry.getValue();
          assertNotNull("Null value for key " + i, v);
          assertEquals(VALUE_SIZE, v.length);
          // test a byte
          assertEquals((byte)0xAB, v[i % VALUE_SIZE]);
        }
        
        rgn.close();
        
        rgn = DiskRegionTestImpl.this.rtc.createRegion(name, attrs2);
        
        // verify
        assertEquals(NUM_ENTRIES + 2, rgn.keySet().size());
        
        if (RECOVER_VALUES) {
          assertEquals(value1, rgn.getEntry(key1).getValue());
          assertEquals(value2, rgn.getEntry(key2).getValue());
        } else {
          assertNull(valueInVM(rgn, key1));
          assertNull(valueInVM(rgn, key2));
        }
        assertEquals(value1, valueOnDisk(rgn, key1));
        assertEquals(value2, valueOnDisk(rgn, key2));
        
        // The following also verifies TEMP values were overwritten
        for (int i = 0; i < NUM_ENTRIES; i++) {
          Region.Entry entry = rgn.getEntry(new Integer(i));
          assertNotNull("No entry for key " + i, entry);
          byte[] v = (byte[])entry.getValue();
          assertNotNull("Null value for key " + i, v);
          assertEquals(VALUE_SIZE, v.length);
          // test a byte
          assertEquals((byte)0xAB, v[i % VALUE_SIZE]);
        }
        
      }
      
    
      
      private Object valueInVM(Region rgn, Object key)
      throws EntryNotFoundException {
        com.gemstone.gemfire.internal.cache.LocalRegion lrgn =
          (com.gemstone.gemfire.internal.cache.LocalRegion)rgn;
        return lrgn.getValueInVM(key);
      }
            
      private Object valueOnDisk(Region rgn, Object key)
      throws EntryNotFoundException {
        com.gemstone.gemfire.internal.cache.LocalRegion lrgn =
          (com.gemstone.gemfire.internal.cache.LocalRegion)rgn;
        return lrgn.getValueOnDisk(key);
      }
    });
  }
  
}
