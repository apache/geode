/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.partitioned.fixed;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static java.util.Locale.US;
import static java.util.Objects.requireNonNull;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.APR;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.AUG;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.DEC;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.FEB;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.JAN;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.JUL;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.JUN;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.MAR;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.MAY;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.NOV;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.OCT;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Month.SEP;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Quarter.Q1;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Quarter.Q2;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Quarter.Q3;
import static org.apache.geode.internal.cache.partitioned.fixed.FixedPartitioningWithColocationAndPersistenceDUnitTest.Quarter.Q4;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.VM.getVMId;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DiskStore;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.FixedPartitionAttributesImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.control.InternalResourceManager;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceObserver;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.PartitioningTest;
import org.apache.geode.test.junit.categories.PersistenceTest;
import org.apache.geode.test.junit.rules.serializable.SerializableTemporaryFolder;

@Category({PartitioningTest.class, PersistenceTest.class})
@SuppressWarnings("serial")
public class FixedPartitioningWithColocationAndPersistenceDUnitTest implements Serializable {

  private static final String QUARTERS_REGION = "quarters";
  private static final String CUSTOMERS_REGION = "customers";
  private static final String ORDERS_REGION = "orders";
  private static final String SHIPMENTS_REGION = "shipments";
  private static final String DISK_STORE = "diskStore";

  private static final InternalCache DUMMY_CACHE = mock(InternalCache.class);

  private static final AtomicReference<InternalCache> CACHE = new AtomicReference<>(DUMMY_CACHE);
  private static final AtomicReference<File> DISK_DIR = new AtomicReference<>();

  private VM vm0;
  private VM vm1;
  private VM vm2;
  private VM vm3;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public SerializableTemporaryFolder temporaryFolder = new SerializableTemporaryFolder();

  @Before
  public void setUp() {
    vm0 = getVM(0);
    vm1 = getVM(1);
    vm2 = getVM(2);
    vm3 = getVM(3);

    for (VM memberVM : asList(vm0, vm1, vm2, vm3)) {
      memberVM.invoke(() -> {
        CACHE.set(DUMMY_CACHE);
        DISK_DIR.set(temporaryFolder.newFolder("diskDir-" + getVMId()).getAbsoluteFile());
      });
    }
  }

  @After
  public void tearDown() {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        InternalResourceManager.setResourceObserver(null);
        closeCache();
        DISK_DIR.set(null);
      });
    }
  }

  /**
   * Validates that in colocation of FPRs child region cannot specify FixedPartitionAttributes
   */
  @Test
  public void testColocation_WithFPROnChildRegion() {
    vm0.invoke(() -> {
      createCache();

      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("Customer100", true, 2))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(8)
          .create(CUSTOMERS_REGION);

      Throwable thrown = catchThrowable(() -> new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("Order100", true, 2))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(8)
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "not be specified in PartitionAttributesFactory if colocated-with is specified");
    });
  }

  /**
   * Validates that in Customer-Order-shipment colocation, Order and shipment have the
   * FixedPartitionAttributes of the parent region Customer.
   *
   * <p>
   * Put happens for all 3 regions. Colocation of the data is achieved by using a partition-resolver
   * {@link CustomerFixedPartitionResolver#getRoutingObject(EntryOperation)}
   * Also the Fixed Partitioning is achieved using same partition-resolver
   * {@link CustomerFixedPartitionResolver#getPartitionName(EntryOperation, Set)}
   *
   * <p>
   * Validation is done for the same number of the buckets on a particular member for all 3 regions.
   */
  @Test
  public void testColocation_FPRs_ChildUsingAttributesOfParent() {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    vm2.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    vm3.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putOrdersData(40);
      putCustomersData(40);
      putShipmentsData(40);

      validateColocatedData(10);
    });

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    }
  }

  @Test
  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent() {
    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm2.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm3.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putOrdersData(40);
      putCustomersData(40);
      putShipmentsData(40);

      validateColocatedData(10);
    });

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    }
  }

  /**
   * This tests validates that Customer-Order-shipment colocation with failover scenario,
   */
  @Test
  public void testColocation_FPRs_ChildUsingAttributesOfParent_HA() {
    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    vm2.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putCustomersData(40);
      putOrdersData(40);
      putShipmentsData(40);

      validateColocatedData(10);
    });

    vm0.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    vm1.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    vm2.invoke(() -> validatePrimaryBucketsForColocation(20, 10));


    vm2.invoke(this::closeCache);

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForColocationAfterCacheClosed(15, 5));
    }

    vm2.invoke(() -> {
      RecoveryFinishedObserver observer =
          new RecoveryFinishedObserver(CUSTOMERS_REGION, ORDERS_REGION, SHIPMENTS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .create(CUSTOMERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm0.invoke(() -> validateColocatedData(10));

    vm2.invoke(() -> validatePrimaryBucketsForColocation(15, 5));

    vm3.invoke(() -> {
      RecoveryFinishedObserver observer =
          new RecoveryFinishedObserver(CUSTOMERS_REGION, ORDERS_REGION, SHIPMENTS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .create(CUSTOMERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm0.invoke(() -> validateColocatedData(10));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    }
  }

  @Test
  public void testColocation_FPR_Persistence_ChildUsingAttributesOfParent_HA() {
    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("30", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm2.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(2)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .persistent(false)
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .persistent(false)
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1, vm2)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putCustomersData(40);
      putOrdersData(40);
      putShipmentsData(40);

      validateColocatedData(10);
    });

    vm0.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    vm1.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    vm2.invoke(() -> validatePrimaryBucketsForColocation(20, 10));

    vm2.invoke(this::closeCache);

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForColocationAfterCacheClosed(15, 5));
    }

    vm2.invoke(() -> {
      RecoveryFinishedObserver observer =
          new RecoveryFinishedObserver(CUSTOMERS_REGION, ORDERS_REGION, SHIPMENTS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("30", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .create(CUSTOMERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm0.invoke(() -> validateColocatedData(10));

    vm2.invoke(() -> validatePrimaryBucketsForColocation(15, 5));

    vm3.invoke(() -> {
      RecoveryFinishedObserver observer =
          new RecoveryFinishedObserver(CUSTOMERS_REGION, ORDERS_REGION, SHIPMENTS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("40", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .create(CUSTOMERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(2)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm0.invoke(() -> validateColocatedData(10));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(15, 5));
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence when one member is kept alive and other
   * members goes down and come up
   */
  @Test
  public void testFPR_Persistence_OneMemberAlive() {
    vm0.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm1.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm0.invoke(() -> {
      putQuartersData(Q1);
      putQuartersData(Q2);
    });

    vm1.invoke(() -> {
      closeCache();
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      validateQuartersData(Q2);
    });

    vm2.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm3.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm0.invoke(() -> putQuartersData());

    vm0.invoke(() -> validatePrimaryData(Q1));
    vm1.invoke(() -> validatePrimaryData(Q2));
    vm2.invoke(() -> validatePrimaryData(Q3));
    vm3.invoke(() -> validatePrimaryData(Q4));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForQuarters(3, 3));
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence when all members goes down and comes up.
   */
  @Test
  public void testFPR_Persistence() {
    vm0.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm1.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm0.invoke(() -> {
      putQuartersData(Q1);
      putQuartersData(Q2);
    });

    vm0.invoke(() -> validatePrimaryData(27 * 2, Q1, Q2));
    vm1.invoke(() -> validatePrimaryData(27 * 2, Q2, Q1));

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForQuarters(6, 3));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::closeCache);
    }

    vm1.invoke(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm1.invoke(() -> {
      validateQuartersData(Q1);
      validateQuartersData(Q2);

      validatePrimaryData(27 * 2, Q2, Q1);
      validatePrimaryBucketsForQuarters(6, 6);
    });

    vm0.invoke(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    vm0.invoke(() -> validatePrimaryData(27 * 2, Q1, Q2));
    vm1.invoke(() -> validatePrimaryData(27 * 2, Q2, Q1));

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForQuarters(6, 3));
    }

    vm0.invoke(() -> validateQuartersData(Q1));
    vm1.invoke(() -> validateQuartersData(Q2));
  }

  /**
   * Tests validate the behavior of FPR with persistence and with colocation when one member is kept
   * alive and other members goes down and come up
   */
  @Test
  public void testColocation_FPR_Persistence_Colocation_OneMemberAlive() {
    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(1)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .redundantCopies(1)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putCustomersData(20, integer -> integer % 2 == 0);
      putOrdersData(20, integer -> integer % 2 == 0);
      putShipmentsData(20, integer -> integer % 2 == 0);

      validateColocatedData(10);
    });

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(10, 5));
    }

    vm1.invoke(this::closeCache);

    vm0.invoke(() -> {
      putCustomersData(20, integer -> integer % 2 == 1);
      putOrdersData(20, integer -> integer % 2 == 1);
      putShipmentsData(20, integer -> integer % 2 == 1);

      validateColocatedData(10);
      validatePrimaryBucketsForColocation(10, 10);
    });

    vm1.invoke(() -> {
      RecoveryFinishedObserver observer =
          new RecoveryFinishedObserver(CUSTOMERS_REGION, ORDERS_REGION, SHIPMENTS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .persistent(true)
          .create(CUSTOMERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION);
      new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(10, 5));
    }
    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validateColocatedData(10));
    }
  }

  /**
   * Tests validate the behavior of FPR with persistence and with colocation when all members goes
   * down and comes up.
   */
  @Test
  public void testColocation_FPR_Persistence_Colocation() {
    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::createCache);
    }

    vm0.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("10", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("20", false, 5))
        .redundantCopies(1)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    vm1.invoke(() -> new RegionBuilder<>()
        .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
        .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
        .redundantCopies(1)
        .localMaxMemory(50)
        .totalNumBuckets(20)
        .partitionResolver(new CustomerFixedPartitionResolver<>())
        .persistent(true)
        .create(CUSTOMERS_REGION));

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(CUSTOMERS_REGION)
          .create(ORDERS_REGION));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .colocatedWith(ORDERS_REGION)
          .create(SHIPMENTS_REGION));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> {
        validateFixedPartitionAttributes(ORDERS_REGION);
        validateFixedPartitionAttributes(SHIPMENTS_REGION);
      });
    }

    vm0.invoke(() -> {
      putCustomersData(20);
      putOrdersData(20);
      putShipmentsData(20);

      validateColocatedData(10);
    });

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForColocation(10, 5));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::closeCache);
    }

    vm1.invoke(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(CUSTOMERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition("20", true, 5))
          .addFixedPartitionAttributes(createFixedPartition("10", false, 5))
          .redundantCopies(1)
          .localMaxMemory(50)
          .totalNumBuckets(20)
          .partitionResolver(new CustomerFixedPartitionResolver<>())
          .persistent(true)
          .create(CUSTOMERS_REGION);

      await().until(observer::isRecoveryFinished);

      validateCustomersData();
    });
  }

  @Test
  public void testFPR_Persistence2() throws InterruptedException {
    vm0.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm1.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm0.invoke(() -> putQuartersData());

    vm0.invoke(() -> validatePrimaryWithSecondaryData(Q1, Q2));
    vm1.invoke(() -> validatePrimaryWithSecondaryData(Q3, Q4));

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> validatePrimaryBucketsForQuarters(6, 6));
    }

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::closeCache);
    }

    AsyncInvocation<Void> createRegionInVM1 = vm1.invokeAsync(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    AsyncInvocation<Void> createRegionInVM0 = vm0.invokeAsync(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .redundantCopies(0)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    createRegionInVM1.await();
    createRegionInVM0.await();

    vm1.invoke(() -> {
      validatePrimaryWithSecondaryData(Q3, Q4);
      validatePrimaryBucketsForQuarters(6, 6);
    });

    vm0.invoke(() -> {
      validatePrimaryWithSecondaryData(Q1, Q2);
      validatePrimaryBucketsForQuarters(6, 6);
    });
  }

  @Test
  public void testFPR_Persistence3() throws InterruptedException {
    vm0.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm1.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm2.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm3.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);
    });

    vm0.invoke(() -> putQuartersData());

    vm0.invoke(() -> validatePrimaryWithSecondaryData(Q1, Q2));
    vm1.invoke(() -> validatePrimaryWithSecondaryData(Q2, Q3));
    vm2.invoke(() -> validatePrimaryWithSecondaryData(Q3, Q4));
    vm3.invoke(() -> validatePrimaryWithSecondaryData(Q4, Q1));

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(() -> validatePrimaryBucketsForQuarters(6, 3));
    }

    for (VM vm : asList(vm0, vm1, vm2, vm3)) {
      vm.invoke(this::closeCache);
    }

    AsyncInvocation<Void> createRegionInVM3 = vm3.invokeAsync(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    AsyncInvocation<Void> createRegionInVM2 = vm2.invokeAsync(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q4.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    AsyncInvocation<Void> createRegionInVM1 = vm1.invokeAsync(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q3.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    AsyncInvocation<Void> createRegionInVM0 = vm0.invokeAsync(() -> {
      RecoveryFinishedObserver observer = new RecoveryFinishedObserver(QUARTERS_REGION);
      InternalResourceManager.setResourceObserver(observer);

      createCache();
      new RegionBuilder<>()
          .addFixedPartitionAttributes(createFixedPartition(Q1.toString(), true, 3))
          .addFixedPartitionAttributes(createFixedPartition(Q2.toString(), false, 3))
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      await().until(observer::isRecoveryFinished);
    });

    createRegionInVM3.await();
    createRegionInVM2.await();
    createRegionInVM1.await();
    createRegionInVM0.await();

    vm3.invoke(() -> {
      validatePrimaryWithSecondaryData(Q4, Q1);
      validatePrimaryBucketsForQuarters(6, 3);
    });

    vm2.invoke(() -> {
      validatePrimaryWithSecondaryData(Q3, Q4);
      validatePrimaryBucketsForQuarters(6, 3);
    });

    vm1.invoke(() -> {
      validatePrimaryWithSecondaryData(Q2, Q3);
      validatePrimaryBucketsForQuarters(6, 3);
    });

    vm0.invoke(() -> {
      validatePrimaryWithSecondaryData(Q1, Q2);
      validatePrimaryBucketsForQuarters(6, 3);
    });
  }

  /**
   * Test validate a normal PR's persistence behavior. normal PR region is created on vm0 and
   * vm1. Put is done on this PR Member1 and Member2's cache is closed respectively. Member2 is
   * brought back and persisted data is verified.
   */
  @Test
  public void testPR_Persistence() {
    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(() -> {
        createCache();
        new RegionBuilder<>()
            .redundantCopies(1)
            .localMaxMemory(40)
            .totalNumBuckets(12)
            .partitionResolver(new QuarterFixedPartitionResolver<>())
            .persistent(true)
            .create(QUARTERS_REGION);
      });
    }

    vm0.invoke(() -> putQuartersData());

    for (VM vm : asList(vm0, vm1)) {
      vm.invoke(this::closeCache);
    }

    vm1.invoke(() -> {
      createCache();
      new RegionBuilder<>()
          .redundantCopies(1)
          .localMaxMemory(40)
          .totalNumBuckets(12)
          .partitionResolver(new QuarterFixedPartitionResolver<>())
          .persistent(true)
          .create(QUARTERS_REGION);

      validateQuartersData();
    });
  }

  private Properties getDistributedSystemProperties() {
    Properties configProperties = DistributedRule.getDistributedSystemProperties();
    configProperties.setProperty(SERIALIZABLE_OBJECT_FILTER,
        String.join(";", "org.apache.geode.internal.cache.functions.**",
            CustomerId.class.getName(), Customer.class.getName(), OrderId.class.getName(),
            Order.class.getName(), ShipmentId.class.getName(), Shipment.class.getName()));
    return configProperties;
  }

  private void createCache() {
    CACHE.set((InternalCache) new CacheFactory(getDistributedSystemProperties()).create());
  }

  private InternalCache getCache() {
    return CACHE.get();
  }

  private void closeCache() {
    CACHE.getAndSet(DUMMY_CACHE).close();
  }

  private PartitionedRegion getPartitionedRegion(String regionName) {
    return (PartitionedRegion) getCache().getRegion(regionName);
  }

  private File[] getDiskDirs() {
    return new File[] {DISK_DIR.get()};
  }

  private void putQuartersData() throws ParseException {
    doPutQuartersData(Month.values());
  }

  private void putQuartersData(Quarter quarter) throws ParseException {
    doPutQuartersData(quarter.months());
  }

  private void doPutQuartersData(Month... months) throws ParseException {
    Region<Date, String> quarterRegion = getCache().getRegion(QUARTERS_REGION);

    for (Month month : months) {
      for (int day = 1; day < 10; day++) {
        Date date = month.date(day);
        String value = month.toString() + day;
        quarterRegion.put(date, value);
      }
    }
  }

  private void putCustomersData(int count) {
    putCustomersData(count, integer -> true);
  }

  private void putCustomersData(int count, Predicate<Integer> predicate) {
    Region<CustomerId, Customer> customerRegion = getCache().getRegion(CUSTOMERS_REGION);

    for (int i = 1; i <= requirePositive(count); i++) {
      if (predicate.test(i)) {
        CustomerId customerId = new CustomerId(i);
        Customer customer = new Customer("Name-" + i, "Address-" + i);
        customerRegion.put(customerId, customer);
      }
    }
  }

  private void putOrdersData(int count) {
    putOrdersData(count, integer -> true);
  }

  private void putOrdersData(int count, Predicate<Integer> predicate) {
    Region<OrderId, Order> orderRegion = getCache().getRegion(ORDERS_REGION);

    for (int i = 1; i <= requirePositive(count); i++) {
      if (predicate.test(i)) {
        CustomerId customerId = new CustomerId(i);

        for (int j = 1; j <= 10; j++) {
          int oid = i * 10 + j;
          OrderId orderId = new OrderId(oid, customerId);
          Order order = new Order("Order-" + oid);
          orderRegion.put(orderId, order);
        }
      }
    }
  }

  private void putShipmentsData(int count) {
    putShipmentsData(count, integer -> true);
  }

  private void putShipmentsData(int count, Predicate<Integer> predicate) {
    Region<ShipmentId, Shipment> shipmentRegion = getCache().getRegion(SHIPMENTS_REGION);

    for (int i = 1; i <= requirePositive(count); i++) {
      if (predicate.test(i)) {
        CustomerId customerId = new CustomerId(i);

        for (int j = 1; j <= 10; j++) {
          int oid = i * 10 + j;
          OrderId orderId = new OrderId(oid, customerId);

          for (int k = 1; k <= 10; k++) {
            int sid = oid * 10 + k;
            ShipmentId shipmentId = new ShipmentId(sid, orderId);
            Shipment shipment = new Shipment("Shipment-" + sid);
            shipmentRegion.put(shipmentId, shipment);
          }
        }
      }
    }
  }

  private <K, V> Region<K, V> getLocalDataSet(Region<K, V> region) {
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    PartitionedRegionDataStore dataStore = partitionedRegion.getDataStore();

    if (dataStore != null) {
      return uncheckedCast(new LocalDataSet(partitionedRegion, dataStore.getAllLocalBucketIds()));
    }
    return uncheckedCast(new LocalDataSet(partitionedRegion, emptySet()));
  }

  private void validateQuartersData() throws ParseException {
    Region<Date, String> quarterRegion = getCache().getRegion(QUARTERS_REGION);

    for (Month month : Month.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = month.date(i);
        String value = month.toString() + i;
        assertThat(quarterRegion.get(date)).isEqualTo(value);
      }
    }
  }

  private void validateQuartersData(Quarter quarter) throws ParseException {
    Region<Date, String> quarterRegion = getCache().getRegion(QUARTERS_REGION);

    for (Month month : quarter.months()) {
      for (int i = 1; i < 10; i++) {
        Date date = month.date(i);
        String value = month.toString() + i;
        assertThat(quarterRegion.get(date)).isEqualTo(value);
      }
    }
  }

  private void validateCustomersData() {
    Region<CustomerId, Customer> customerRegion = getCache().getRegion(CUSTOMERS_REGION);

    for (int id = 1; id <= 20; id++) {
      CustomerId customerId = new CustomerId(id);
      Customer customer = new Customer("Name-" + id, "Address-" + id);
      assertThat(customerRegion.get(customerId)).isEqualTo(customer);
    }

    assertThat(getCache().getRegion(ORDERS_REGION)).isNull();
    assertThat(getCache().getRegion(SHIPMENTS_REGION)).isNull();
  }

  private void validateColocatedData(int count) {
    PartitionedRegion customers = (PartitionedRegion) getCache().getRegion(CUSTOMERS_REGION);
    PartitionedRegion orders = (PartitionedRegion) getCache().getRegion(ORDERS_REGION);
    PartitionedRegion shipments = (PartitionedRegion) getCache().getRegion(SHIPMENTS_REGION);

    for (int i = 0; i < requirePositive(count); i++) {
      InternalDistributedMember idmForCustomer = customers.getBucketPrimary(i);
      InternalDistributedMember idmForOrder = orders.getBucketPrimary(i);
      InternalDistributedMember idmForShipment = shipments.getBucketPrimary(i);

      // take all the keys from the shipment for each bucket
      Set<CustomerId> customerKey = uncheckedCast(customers.getBucketKeys(i));
      assertThat(customerKey).isNotNull();

      for (CustomerId customerId : customerKey) {
        assertThat(customers.get(customerId)).isNotNull();

        Set<OrderId> orderKey = uncheckedCast(orders.getBucketKeys(i));
        for (OrderId orderId : orderKey) {
          assertThat(orders.get(orderId)).isNotNull();
          if (orderId.getCustomerId().equals(customerId)) {
            assertThat(idmForOrder).isEqualTo(idmForCustomer);
          }

          Set<ShipmentId> shipmentKey = uncheckedCast(shipments.getBucketKeys(i));
          for (ShipmentId shipmentId : shipmentKey) {
            assertThat(shipments.get(shipmentId)).isNotNull();
            if (shipmentId.getOrderId().equals(orderId)) {
              assertThat(idmForShipment).isEqualTo(idmForOrder);
            }
          }
        }
      }
    }
  }

  private void validatePrimaryData(Quarter partitionQuarter)
      throws ParseException {
    Region<Date, String> localDataSet = getLocalDataSet(getCache().getRegion(QUARTERS_REGION));
    doValidatePrimaryData(localDataSet, partitionQuarter);
  }

  private void validatePrimaryData(int expectedLocalDataCount, Quarter... partitionQuarters)
      throws ParseException {
    Region<Date, String> localDataSet = getLocalDataSet(getCache().getRegion(QUARTERS_REGION));
    assertThat(localDataSet).hasSize(expectedLocalDataCount);
    doValidatePrimaryData(localDataSet, partitionQuarters);
  }

  private void doValidatePrimaryData(Map<Date, String> localDataSet, Quarter... expectedQuarters)
      throws ParseException {
    Collection<Quarter> unexpectedQuarters = new ArrayList<>(asList(Quarter.values()));
    for (Quarter quarter : Quarter.values()) {
      for (Quarter expectedQuarter : expectedQuarters) {
        if (quarter == expectedQuarter) {
          validateDataContainsQuarter(localDataSet, quarter);
          unexpectedQuarters.remove(quarter);
        }
      }
    }

    for (Quarter unexpectedQuarter : unexpectedQuarters) {
      validateDataDoesNotContainQuarter(localDataSet, unexpectedQuarter);
    }
  }

  private void validatePrimaryWithSecondaryData(Quarter... partitionQuarters)
      throws ParseException {
    Region<Date, String> localDataSet = getLocalDataSet(getCache().getRegion(QUARTERS_REGION));
    doValidatePrimaryData(localDataSet, partitionQuarters);
  }

  private void validateDataContainsQuarter(Map<Date, String> localDataSet, Quarter quarter)
      throws ParseException {
    for (Month month : quarter.months()) {
      for (int day = 1; day < 10; day++) {
        Date date = month.date(day);
        assertThat(localDataSet.keySet()).contains(date);
      }
    }
  }

  private void validateDataDoesNotContainQuarter(Map<Date, String> localDataSet, Quarter quarter)
      throws ParseException {
    for (Month month : quarter.months()) {
      for (int day = 1; day < 10; day++) {
        Date date = month.date(day);
        assertThat(localDataSet.keySet()).doesNotContain(date);
      }
    }
  }

  private void validatePrimaryBucketsForQuarters(int bucketCount, int primaryBucketCount) {
    PartitionedRegionDataStore localDataStore =
        getPartitionedRegion(QUARTERS_REGION).getDataStore();

    assertThat(localDataStore.getSizeLocally())
        .hasSize(bucketCount);
    assertThat(localDataStore.getNumberOfPrimaryBucketsManaged())
        .isEqualTo(primaryBucketCount);
  }

  private void validatePrimaryBucketsForColocation(int bucketCount, int primaryBucketCount) {
    PartitionedRegion customerPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(CUSTOMERS_REGION);
    PartitionedRegion orderPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(ORDERS_REGION);
    PartitionedRegion shipmentPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(SHIPMENTS_REGION);

    Map<Integer, Integer> localBucket2RegionMap_Customer =
        customerPartitionedRegion.getDataStore().getSizeLocally();
    Map<Integer, Integer> localBucket2RegionMap_Order =
        orderPartitionedRegion.getDataStore().getSizeLocally();
    Map<Integer, Integer> localBucket2RegionMap_Shipment =
        shipmentPartitionedRegion.getDataStore().getSizeLocally();

    assertThat(localBucket2RegionMap_Customer)
        .hasSize(bucketCount);
    assertThat(localBucket2RegionMap_Order)
        .hasSize(bucketCount);
    assertThat(localBucket2RegionMap_Shipment)
        .hasSize(bucketCount);

    assertThat(localBucket2RegionMap_Order.keySet())
        .isEqualTo(localBucket2RegionMap_Customer.keySet());
    assertThat(localBucket2RegionMap_Shipment.keySet())
        .isEqualTo(localBucket2RegionMap_Customer.keySet());
    assertThat(localBucket2RegionMap_Shipment.keySet())
        .isEqualTo(localBucket2RegionMap_Order.keySet());

    List<Integer> primaryBuckets_Customer =
        customerPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();
    List<Integer> primaryBuckets_Order =
        orderPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();
    List<Integer> primaryBuckets_Shipment =
        shipmentPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();

    assertThat(primaryBuckets_Customer)
        .hasSize(primaryBucketCount);
    assertThat(primaryBuckets_Order)
        .hasSize(primaryBucketCount);
    assertThat(primaryBuckets_Shipment)
        .hasSize(primaryBucketCount);

    assertThat(primaryBuckets_Order)
        .isEqualTo(primaryBuckets_Customer);
    assertThat(primaryBuckets_Shipment)
        .isEqualTo(primaryBuckets_Customer);
    assertThat(primaryBuckets_Shipment)
        .isEqualTo(primaryBuckets_Order);
  }

  private void validatePrimaryBucketsForColocationAfterCacheClosed(int bucketCount,
      int primaryBucketCount) {
    PartitionedRegion customerPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(CUSTOMERS_REGION);
    PartitionedRegion orderPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(ORDERS_REGION);
    PartitionedRegion shipmentPartitionedRegion =
        (PartitionedRegion) getCache().getRegion(SHIPMENTS_REGION);

    Map<Integer, Integer> localBucket2RegionMap_Customer =
        customerPartitionedRegion.getDataStore().getSizeLocally();
    Map<Integer, Integer> localBucket2RegionMap_Order =
        orderPartitionedRegion.getDataStore().getSizeLocally();
    Map<Integer, Integer> localBucket2RegionMap_Shipment =
        shipmentPartitionedRegion.getDataStore().getSizeLocally();

    assertThat(localBucket2RegionMap_Customer)
        .hasSize(bucketCount);
    assertThat(localBucket2RegionMap_Order)
        .hasSize(bucketCount);
    assertThat(localBucket2RegionMap_Shipment)
        .hasSize(bucketCount);

    assertThat(localBucket2RegionMap_Order.keySet())
        .isEqualTo(localBucket2RegionMap_Customer.keySet());
    assertThat(localBucket2RegionMap_Shipment.keySet())
        .isEqualTo(localBucket2RegionMap_Customer.keySet());
    assertThat(localBucket2RegionMap_Shipment.keySet())
        .isEqualTo(localBucket2RegionMap_Order.keySet());

    List<Integer> primaryBuckets_Customer =
        customerPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();
    List<Integer> primaryBuckets_Order =
        orderPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();
    List<Integer> primaryBuckets_Shipment =
        shipmentPartitionedRegion.getDataStore().getLocalPrimaryBucketsListTestOnly();

    assertThat(primaryBuckets_Customer.size() % primaryBucketCount)
        .as("primaryBuckets_Customer.size() % primaryBucketCount")
        .isZero();
    assertThat(primaryBuckets_Order.size() % primaryBucketCount)
        .as("primaryBuckets_Order.size() % primaryBucketCount")
        .isZero();
    assertThat(primaryBuckets_Shipment.size() % primaryBucketCount)
        .as("primaryBuckets_Shipment.size() % primaryBucketCount")
        .isZero();

    assertThat(primaryBuckets_Order).isEqualTo(primaryBuckets_Customer);
    assertThat(primaryBuckets_Shipment).isEqualTo(primaryBuckets_Customer);
    assertThat(primaryBuckets_Shipment).isEqualTo(primaryBuckets_Order);
  }

  private void validateFixedPartitionAttributes(String regionName) {
    PartitionedRegion partitionedRegion = (PartitionedRegion) getCache().getRegion(regionName);
    PartitionedRegion colocatedRegion =
        (PartitionedRegion) getCache().getRegion(partitionedRegion.getColocatedWith());

    List<FixedPartitionAttributesImpl> childAttributes =
        partitionedRegion.getFixedPartitionAttributesImpl();
    List<FixedPartitionAttributesImpl> parentAttributes =
        colocatedRegion.getFixedPartitionAttributesImpl();

    assertThat(childAttributes).isEqualTo(parentAttributes);
  }

  private static int requirePositive(int number) {
    if (number < 0) {
      throw new IllegalArgumentException("Number '" + number + "' is negative.");
    }
    return number;
  }

  enum Quarter {
    Q1(JAN, FEB, MAR), Q2(APR, MAY, JUN), Q3(JUL, AUG, SEP), Q4(OCT, NOV, DEC);

    private final List<Month> months;

    Quarter(Month... months) {
      this.months = asList(months);
    }

    Month[] months() {
      return months.toArray(new Month[0]);
    }

    static Quarter find(Month month) {
      for (Quarter quarter : values()) {
        if (quarter.months.contains(month)) {
          return quarter;
        }
      }
      throw new IllegalArgumentException("Quarter not found for month '" + month + "'");
    }
  }

  enum Month {
    JAN(1),
    FEB(2),
    MAR(3),
    APR(4),
    MAY(5),
    JUN(6),
    JUL(7),
    AUG(8),
    SEP(9),
    OCT(10),
    NOV(11),
    DEC(12);

    private final int id;

    Month(int id) {
      this.id = id;
    }

    Date date(int day) throws ParseException {
      String dayString = day > 0 && day < 10 ? "0" + day : String.valueOf(day);
      String dateString = dayString + "-" + this + "-2010";
      SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy", US);
      return dateFormat.parse(dateString);
    }

    static Month find(int id) {
      for (Month month : values()) {
        if (month.id == id) {
          return month;
        }
      }
      throw new IllegalArgumentException("Month not found for id '" + id + "'");
    }
  }

  private class RegionBuilder<K, V> {

    private final Collection<FixedPartitionAttributes> fixedPartitionAttributesList =
        new ArrayList<>();
    private int redundantCopies;
    private int localMaxMemory;
    private int totalNumBuckets;
    private PartitionResolver<K, V> partitionResolver;
    private String colocatedWith;
    private boolean persistent;

    private RegionBuilder<K, V> addFixedPartitionAttributes(
        FixedPartitionAttributes fixedPartitionAttributes) {
      fixedPartitionAttributesList.add(fixedPartitionAttributes);
      return this;
    }

    private RegionBuilder<K, V> redundantCopies(int redundantCopies) {
      this.redundantCopies = redundantCopies;
      return this;
    }

    private RegionBuilder<K, V> localMaxMemory(int localMaxMemory) {
      this.localMaxMemory = localMaxMemory;
      return this;
    }

    private RegionBuilder<K, V> totalNumBuckets(int totalNumBuckets) {
      this.totalNumBuckets = totalNumBuckets;
      return this;
    }

    private RegionBuilder<K, V> partitionResolver(PartitionResolver<K, V> partitionResolver) {
      this.partitionResolver = partitionResolver;
      return this;
    }

    private RegionBuilder<K, V> colocatedWith(String colocatedWith) {
      this.colocatedWith = colocatedWith;
      return this;
    }

    private RegionBuilder<K, V> persistent(boolean persistent) {
      this.persistent = persistent;
      return this;
    }

    private void create(String regionName) {
      PartitionAttributesFactory<K, V> paf = new PartitionAttributesFactory<K, V>()
          .setRedundantCopies(redundantCopies)
          .setLocalMaxMemory(localMaxMemory)
          .setTotalNumBuckets(totalNumBuckets)
          .setPartitionResolver(partitionResolver)
          .setColocatedWith(colocatedWith);

      for (FixedPartitionAttributes fixedPartitionAttributes : fixedPartitionAttributesList) {
        paf.addFixedPartitionAttributes(fixedPartitionAttributes);
      }

      RegionFactory<K, V> regionFactory;
      if (persistent) {
        regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION_PERSISTENT);

        DiskStore diskStore = getCache().findDiskStore(DISK_STORE);
        if (diskStore == null) {
          diskStore = getCache().createDiskStoreFactory()
              .setDiskDirs(getDiskDirs())
              .create(DISK_STORE);
        }
        regionFactory.setDiskStoreName(diskStore.getName());

      } else {
        regionFactory = getCache().createRegionFactory(RegionShortcut.PARTITION);
      }

      regionFactory
          .setPartitionAttributes(paf.create())
          .create(regionName);
    }
  }

  private static class RecoveryFinishedObserver implements ResourceObserver {

    private final Map<String, AtomicBoolean> regionNames = new HashMap<>();

    private RecoveryFinishedObserver(String... regionNames) {
      for (String regionName : regionNames) {
        this.regionNames.put(requireNonNull(regionName), new AtomicBoolean());
      }
    }

    private boolean isRecoveryFinished() {
      for (Map.Entry<String, AtomicBoolean> entry : regionNames.entrySet()) {
        if (!entry.getValue().get()) {
          return false;
        }
      }
      return true;
    }

    @Override
    public void rebalancingStarted(Region region) {
      // nothing
    }

    @Override
    public void rebalancingFinished(Region region) {
      // nothing
    }

    @Override
    public void recoveryStarted(Region region) {
      // nothing
    }

    @Override
    public void recoveryFinished(Region region) {
      for (Map.Entry<String, AtomicBoolean> entry : regionNames.entrySet()) {
        if (entry.getKey().equals(region.getName())) {
          entry.getValue().set(true);
        }
      }
    }

    @Override
    public void recoveryConflated(PartitionedRegion region) {
      // nothing
    }

    @Override
    public void movingBucket(Region region, int bucketId, DistributedMember source,
        DistributedMember target) {
      // nothing
    }

    @Override
    public void movingPrimary(Region region, int bucketId, DistributedMember source,
        DistributedMember target) {
      // nothing
    }
  }

  private static class CustomerId implements Serializable {

    private final int id;

    private CustomerId(int id) {
      this.id = id;
    }

    private int getId() {
      return id;
    }

    @Override
    public String toString() {
      return "CustomerId {id=" + id + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof CustomerId)) {
        return false;
      }
      CustomerId other = (CustomerId) o;
      return other.id == id;
    }

    @Override
    public int hashCode() {
      return id;
    }
  }

  private static class Customer implements Serializable {

    private final String name;
    private final String address;

    private Customer(String name, String address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public String toString() {
      return "Customer {name=" + name + ", address=" + address + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Customer)) {
        return false;
      }
      Customer other = (Customer) o;
      return other.name.equals(name) && other.address.equals(address);
    }

    @Override
    public int hashCode() {
      return name.hashCode() + address.hashCode();
    }
  }

  private static class OrderId implements Serializable {

    private final int id;
    private final CustomerId customerId;

    private OrderId(int id, CustomerId customerId) {
      this.id = id;
      this.customerId = customerId;
    }

    private CustomerId getCustomerId() {
      return customerId;
    }

    @Override
    public String toString() {
      return "OrderId {id=" + id + ", " + customerId + "}";
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof OrderId)) {
        return false;
      }
      OrderId other = (OrderId) o;
      return other.id == id && other.customerId.equals(customerId);
    }

    @Override
    public int hashCode() {
      return customerId.hashCode();
    }
  }

  private static class Order implements Serializable {

    private final String orderName;

    private Order(String orderName) {
      this.orderName = orderName;
    }

    @Override
    public String toString() {
      return "Order {orderName=" + orderName + "}";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Order)) {
        return false;
      }
      Order other = (Order) obj;
      return other.orderName != null && other.orderName.equals(orderName);
    }

    @Override
    public int hashCode() {
      return orderName.hashCode();
    }
  }

  private static class ShipmentId implements Serializable {

    private final int id;
    private final OrderId orderId;

    private ShipmentId(int id, OrderId orderId) {
      this.id = id;
      this.orderId = orderId;
    }

    private OrderId getOrderId() {
      return orderId;
    }

    @Override
    public String toString() {
      return "ShipmentId {id=" + id + ", orderId=" + orderId + "}";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof ShipmentId)) {
        return false;
      }
      ShipmentId other = (ShipmentId) obj;
      return orderId.equals(other.orderId) && id == other.id;
    }

    @Override
    public int hashCode() {
      return orderId.getCustomerId().hashCode();
    }
  }

  private static class Shipment implements Serializable {

    private final String shipmentName;

    private Shipment(String shipmentName) {
      this.shipmentName = shipmentName;
    }

    @Override
    public String toString() {
      return "Shipment {shipmentName=" + shipmentName + "}";
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof Shipment)) {
        return false;
      }
      Shipment other = (Shipment) obj;
      return other.shipmentName != null && other.shipmentName.equals(shipmentName);
    }

    @Override
    public int hashCode() {
      return shipmentName.hashCode();
    }
  }

  public static class CustomerFixedPartitionResolver<K, V>
      implements FixedPartitionResolver<K, V>, Serializable {

    @Override
    public String getPartitionName(EntryOperation<K, V> opDetails, Set<String> targetPartitions) {
      int customerId = -1;

      if (opDetails.getKey() instanceof ShipmentId) {
        ShipmentId key = (ShipmentId) opDetails.getKey();
        customerId = key.getOrderId().getCustomerId().getId();

      } else if (opDetails.getKey() instanceof OrderId) {
        OrderId key = (OrderId) opDetails.getKey();
        customerId = key.getCustomerId().getId();

      } else if (opDetails.getKey() instanceof CustomerId) {
        CustomerId key = (CustomerId) opDetails.getKey();
        customerId = key.getId();
      }

      if (customerId >= 1 && customerId <= 10) {
        return "10";
      }
      if (customerId > 10 && customerId <= 20) {
        return "20";
      }
      if (customerId > 20 && customerId <= 30) {
        return "30";
      }
      if (customerId > 30 && customerId <= 40) {
        return "40";
      }
      return "Invalid";
    }

    @Override
    public Serializable getRoutingObject(EntryOperation<K, V> opDetails) {
      Serializable routingObject = null;

      if (opDetails.getKey() instanceof ShipmentId) {
        ShipmentId key = (ShipmentId) opDetails.getKey();
        routingObject = key.getOrderId().getCustomerId();

      } else if (opDetails.getKey() instanceof OrderId) {
        OrderId key = (OrderId) opDetails.getKey();
        routingObject = key.getCustomerId();

      } else if (opDetails.getKey() instanceof CustomerId) {
        CustomerId key = (CustomerId) opDetails.getKey();
        routingObject = key.getId();
      }

      return routingObject;
    }

    @Override
    public String getName() {
      return getClass().getSimpleName();
    }
  }

  public static class QuarterFixedPartitionResolver<K, V>
      implements FixedPartitionResolver<K, V>, Serializable {

    private final Properties properties = new Properties();

    private QuarterFixedPartitionResolver() {
      properties.setProperty("routingType", "key");
    }

    @Override
    public String getPartitionName(EntryOperation<K, V> opDetails, Set<String> targetPartitions) {
      Date date = (Date) opDetails.getKey();
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);

      Month month = Month.find(calendar.get(Calendar.MONTH) + 1);

      return Quarter.find(month).toString();
    }

    @Override
    public Serializable getRoutingObject(EntryOperation<K, V> opDetails) {
      Date date = (Date) opDetails.getKey();
      Calendar calendar = Calendar.getInstance();
      calendar.setTime(date);
      return calendar.get(Calendar.MONTH);
    }

    @Override
    public String getName() {
      return getClass().getSimpleName();
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof QuarterFixedPartitionResolver)) {
        return false;
      }
      QuarterFixedPartitionResolver other = (QuarterFixedPartitionResolver) obj;
      return properties.equals(other.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(properties);
    }
  }
}
