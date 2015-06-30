/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.*;

import com.gemstone.gemfire.*;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.CacheException;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache30.CacheSerializableRunnable;
import com.gemstone.gemfire.cache30.CacheTestCase;

import dunit.Host;
import dunit.SerializableCallable;
import dunit.SerializableRunnable;
import dunit.VM;

/**
 * Tests a toData that always throws an IOException.
 * This test does a put with the bad to data from an accessor
 * to see if it will keep trying to resend the put to the data store
 * 
 * @author darrel
 * 
 */
public class PRBadToDataDUnitTest extends CacheTestCase {

  public PRBadToDataDUnitTest(final String name) {
    super(name);
  }

  public void testBadToData() {
    final Host host = Host.getHost(0);
    final VM vm1 = host.getVM(0);
    final VM vm2 = host.getVM(1);
    final String name = "PR_TEMP";

    final SerializableRunnable create = new CacheSerializableRunnable(
        "Create PR accessor ") {
      public void run2() {
        final AttributesFactory factory = new AttributesFactory();
        factory.setPartitionAttributes(new PartitionAttributesFactory()
                                       .setRedundantCopies(0)
                                       .setLocalMaxMemory(0).create());
        final PartitionedRegion pr = (PartitionedRegion)createRootRegion(name,
            factory.create());
        assertNotNull(pr);
      }
    };
    vm1.invoke(create);

    final SerializableRunnable create2 = new SerializableRunnable(
        "Create PR dataStore ") {
      public void run() {
        try {
          final AttributesFactory factory = new AttributesFactory();
          factory.setPartitionAttributes(new PartitionAttributesFactory()
                                         .setRedundantCopies(0)
                                         .setLocalMaxMemory(100).create());
          final PartitionedRegion pr = (PartitionedRegion)createRootRegion(
              name, factory.create());
          assertNotNull(pr);
        }
        catch (final CacheException ex) {
          fail("While creating Partitioned region", ex);
        }
      }
    };
    vm2.invoke(create2);

    final SerializableRunnable putData = new SerializableRunnable("Puts Data") {
        public void run() {
          final PartitionedRegion pr = (PartitionedRegion)getRootRegion(name);
          assertNotNull(pr);
          try {
            pr.put("key", new DataSerializable() {
                public void toData(DataOutput out) throws IOException {
                  throw new IOException("bad to data");
                  //throw new ToDataException("bad to data");
                }
                public void fromData(DataInput in)
                  throws IOException, ClassNotFoundException {
                  // nothing needed
                }
              });
            fail("expected ToDataException");
          } catch (ToDataException expected) {
            // we want this put to fail with an exception instead of hanging due to retries
          }
        }
      };
    vm1.invoke(putData);
  }
}
