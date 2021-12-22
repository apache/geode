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
import static java.util.Collections.singletonList;
import static org.apache.geode.cache.FixedPartitionAttributes.createFixedPartition;
import static org.apache.geode.cache.RegionShortcut.PARTITION;
import static org.apache.geode.cache.partition.PartitionRegionHelper.getLocalData;
import static org.apache.geode.distributed.ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER;
import static org.apache.geode.test.dunit.IgnoredException.addIgnoredException;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.apache.geode.test.dunit.rules.DistributedRule.getDistributedSystemProperties;
import static org.apache.geode.util.internal.GeodeGlossary.GEMFIRE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.assertj.core.api.Assertions.fail;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.DuplicatePrimaryPartitionException;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.FixedPartitionResolver;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.PartitionedRegionStorageException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.control.RebalanceOperation;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.partition.PartitionNotAvailableException;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.PartitionedRegionDataStore;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverAdapter;
import org.apache.geode.internal.cache.partitioned.PartitionedRegionObserverHolder;
import org.apache.geode.internal.cache.xmlcache.Declarable2;
import org.apache.geode.test.dunit.AsyncInvocation;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.Wait;
import org.apache.geode.test.dunit.rules.DistributedBlackboard;
import org.apache.geode.test.dunit.rules.DistributedDiskDirRule;
import org.apache.geode.test.dunit.rules.DistributedErrorCollector;
import org.apache.geode.test.dunit.rules.DistributedReference;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;
import org.apache.geode.test.dunit.rules.DistributedRule;
import org.apache.geode.test.junit.categories.PartitioningTest;

/**
 * This Dunit test class have multiple tests to tests different validations of static partitioning
 */
@Category(PartitioningTest.class)
@SuppressWarnings({"serial", "CodeBlock2Expr", "SerializableStoresNonSerializable",
    "UseOfObsoleteDateTimeApi"})
public class FixedPartitioningDUnitTest implements Serializable {

  private static final String REGION_NAME = "Quarter";

  private static final String QUARTER_1 = "Q1";
  private static final String QUARTER_2 = "Q2";
  private static final String QUARTER_3 = "Q3";
  private static final String QUARTER_4 = "Q4";

  private VM member1;
  private VM member2;
  private VM member3;
  private VM member4;

  @Rule
  public DistributedRule distributedRule = new DistributedRule();
  @Rule
  public DistributedReference<Cache> cache = new DistributedReference<>();
  @Rule
  public DistributedDiskDirRule diskDirRule = new DistributedDiskDirRule();
  @Rule
  public DistributedRestoreSystemProperties restoreProps = new DistributedRestoreSystemProperties();
  @Rule
  public DistributedErrorCollector errorCollector = new DistributedErrorCollector();
  @Rule
  public DistributedBlackboard blackboard = new DistributedBlackboard();

  @Before
  public void setUp() {
    member1 = getVM(0);
    member2 = getVM(1);
    member3 = getVM(2);
    member4 = getVM(3);
  }

  @After
  public void tearDown() {
    for (VM vm : asList(getVM(0), getVM(1), getVM(2), getVM(3))) {
      vm.invoke(() -> {
        PartitionedRegion.BEFORE_CALCULATE_STARTING_BUCKET_FLAG = false;
        PartitionedRegionObserverHolder.clear();
      });
    }
  }

  /**
   * This test validates that null partition name cannot be added in FixedPartitionAttributes
   */
  @Test
  public void testNullPartitionName() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(null, true, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 3,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Fixed partition name cannot be null");
    });
  }

  /**
   * This tests validate that same partition name cannot be added more than once as primary as well
   * as secondary on same member.
   */
  @Test
  public void testSamePartitionNameTwice() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 0, 40, 3,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("can be added only once in FixedPartitionAttributes");
    });
  }

  /**
   * This test validates that FixedPartitionAttributes cannot be defined for accessor nodes
   */
  @Test
  public void testFixedPartitionAttributes_Accessor() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 0, 0, 3,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("can not be defined for accessor");
    });
  }

  /**
   * Test validation : only one node should return primary for a particular partition name for a
   * specific FPR at any given point of time. DuplicatePrimaryPartitionException is thrown during
   * FPR creation if this condition is not met.
   */
  @Test
  public void testSamePartitionName_Primary_OnTwoMembers() {
    addIgnoredException(DuplicatePrimaryPartitionException.class);

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 9,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 9,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 9,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(DuplicatePrimaryPartitionException.class)
          .hasMessageContaining("can not be defined as primary on more than one node");
    });
  }

  /**
   * Test validation : if same partition is having different num-buckets across the nodes then
   * illegalStateException will be thrown
   */
  @Test
  public void testSamePartitionName_DifferentNumBuckets() {
    addIgnoredException(IllegalStateException.class);

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 9,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 8);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 9,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("num-buckets are not same");
    });
  }

  /**
   * Number of primary partitions (which should be one for a partition) and secondary partitions of
   * a FPR for a partition should never exceed number of redundant copies + 1. IllegalStateException
   * is thrown during FPR creation if this condition is not met.
   */
  @Test
  public void testNumberOfPartitions() {
    addIgnoredException(IllegalStateException.class);

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion(REGION_NAME, null, 1, 0, 9, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition("Q11", true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition("Q12", false, 3);
      createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 9,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition("Q12", true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition("Q13", false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition("Q11", false, 3);
      createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2, fpa3), 1, 40, 9,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition("Q13", true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition("Q11", false, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 9,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("should never exceed number of redundant copies");
    });
  }

  /**
   * Sum of num-buckets for different primary partitions should not be greater than totalNumBuckets.
   */
  @Test
  public void testNumBuckets_totalNumBuckets() {
    addIgnoredException(IllegalStateException.class);

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion(REGION_NAME, null, 1, 0, 5, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 5,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, asList(fpa1, fpa2), 1, 40, 5,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining(
              "for different primary partitions should not be greater than total-num-buckets");
    });
  }

  /**
   * This test validates that if the required partition is not available at the time of entry
   * operation then PartitionNotAvailableException is thrown
   */
  @Test
  public void testPut_PartitionNotAvailableException() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion(REGION_NAME, null, 1, 0, 12, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putThroughAccessor(REGION_NAME);
      });

      assertThat(thrown)
          .isInstanceOf(PartitionNotAvailableException.class);
    });
  }

  /**
   * This test validates that if one datastore has the fixed partition attributes defined then other
   * datastore should also have the fixed partition attributes defined
   */
  @Test
  public void test_DataStoreWithoutPartition_DataStoreWithPartition() {
    addIgnoredException(IllegalStateException.class);

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion(REGION_NAME, null, 1, 40, 12, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, singletonList(fpa), 1, 40, 12,
            newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class);
    });
  }

  /**
   * This test validates that if one datastore has the fixed partition attributes defined then other
   * datastore should also have the fixed partition attributes defined
   */
  @Test
  public void test_DataStoreWithPartition_DataStoreWithoutPartition() {
    addIgnoredException(IllegalStateException.class);

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());

      Throwable thrown = catchThrowable(() -> {
        createPartitionedRegion(REGION_NAME, null, 1, 40, 12, newQuarterPartitionResolver());
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class);
    });
  }

  /**
   * This tests validate that accessor member does the put on datastores as per primary
   * FixedPartitionAttributes defined on datastores
   */
  @Test
  public void testPut_ValidateDataOnMember_OnlyPrimary_Accessor() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion(REGION_NAME, null, 0, 0, 12, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion(REGION_NAME, singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughAccessor(REGION_NAME));

    member2.invoke(() -> checkPrimaryData(QUARTER_1));
    member3.invoke(() -> checkPrimaryData(QUARTER_2));
    member4.invoke(() -> checkPrimaryData(QUARTER_3));
  }

  @Test
  public void putWhileDataStoresAreBeingCreatedFails() throws Exception {
    VM accessor = getVM(0);
    VM datastore1 = getVM(1);
    VM datastore2 = getVM(2);
    VM datastore3 = getVM(3);

    accessor.invoke(() -> cache.set(new CacheFactory(getDistributedSystemProperties()).create()));
    datastore1.invoke(() -> cache.set(new CacheFactory(getDistributedSystemProperties()).create()));
    datastore2.invoke(() -> cache.set(new CacheFactory(getDistributedSystemProperties()).create()));
    datastore3.invoke(() -> cache.set(new CacheFactory(getDistributedSystemProperties()).create()));

    blackboard.initBlackboard();

    // Create an accessor for the PR
    accessor.invoke(
        () -> createPartitionedRegion(REGION_NAME, null, 0, 0, 12, newQuarterPartitionResolver()));

    // Set an observer in datastore 1 that will wait in the middle of PR creation
    datastore1.invoke(this::setPRObserverBeforeCalculateStartingBucketId);

    // Start datastore1 asynchronously. This will get stuck waiting in the PR observer
    AsyncInvocation createRegionInDatastore1 =
        datastore1.invokeAsync(() -> createRegionWithQuarter(QUARTER_1));

    // Make sure datastore 1 gets stuck waiting on the observer
    blackboard.waitForGate("waiting");

    // Start two more data stores asynchronously. These will get stuck waiting for a dlock behind
    // datastore1
    AsyncInvocation createRegionInDatastore2 =
        datastore2.invokeAsync(() -> createRegionWithQuarter(QUARTER_2));
    AsyncInvocation createRegionInDatastore3 =
        datastore3.invokeAsync(() -> createRegionWithQuarter(QUARTER_3));

    // Make sure a put to datastore1, which is in the middle of pr initialization, fails correctly
    accessor.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putForQuarter(REGION_NAME, QUARTER_1);
      });

      assertThat(thrown)
          .isInstanceOf(PartitionedRegionStorageException.class);
    });

    // Make sure a put to datastore2, which stuck behind datastore1 in initialization, fails
    // correctly
    accessor.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putForQuarter(REGION_NAME, QUARTER_2);
      });

      assertThat(thrown)
          .isInstanceOf(PartitionedRegionStorageException.class);
    });

    blackboard.signalGate("done");

    createRegionInDatastore1.await();
    createRegionInDatastore2.await();
    createRegionInDatastore3.await();
  }

  /**
   * This tests validate that datastore member does the put on itself as well as other datastores as
   * per primary FixedPartitionAttributes defined on datastores.
   */
  @Test
  public void testPut_ValidateDataOnMember_OnlyPrimary_Datastore() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimaryData(QUARTER_1));
    member2.invoke(() -> checkPrimaryData(QUARTER_2));
    member3.invoke(() -> checkPrimaryData(QUARTER_3));
    member4.invoke(() -> checkPrimaryData(QUARTER_4));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
  }

  /**
   * This test validate that a delete operation on empty region will throw EntryNotFoundException
   */
  @Test
  public void testDelete_WithoutPut() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        deleteOperation("Quarter");
      });

      assertThat(thrown)
          .isInstanceOf(EntryNotFoundException.class);

      putThroughDataStore("Quarter");
      getThroughDataStore("Quarter");
    });
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. But No resolver is provided. So
   * IllegalStateException in expected
   */
  @Test
  public void testPut_NoResolver() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putThroughDataStore_NoResolver("Quarter");
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class);
    });
  }

  /**
   * This tests validate that datastore member tries the put with callback on itself as well as
   * other datastores as per primary FixedPartitionAttributes defined on datastores. here CallBack
   * implements FixedPartitionResolver.
   */
  @Test
  public void testPut_CallBackWithResolver() {
    Properties props = getDistributedSystemProperties();
    props.setProperty(SERIALIZABLE_OBJECT_FILTER, "*");

    member1.invoke(() -> {
      cache.set(new CacheFactory(props).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(props).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(props).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(props).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member1.invoke(() -> putThroughDataStore_CallBackWithResolver("Quarter"));

    member1.invoke(() -> checkPrimaryData(QUARTER_1));
    member2.invoke(() -> checkPrimaryData(QUARTER_2));
    member3.invoke(() -> checkPrimaryData(QUARTER_3));
    member4.invoke(() -> checkPrimaryData(QUARTER_4));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
  }

  /**
   * This test validates that a PR without Fixed Partition Attributes and with
   * FixedPartitionResolver will do custom partitioning as per resolver.
   */
  @Test
  public void testPut_WithResolver_NoFPAs() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", null, 0, 40, 12, newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", null, 0, 40, 12, newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", null, 0, 40, 12, newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", null, 0, 40, 12, newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. Here No Resolver is provided
   * through attributes. Some keys implement FixedPartitionResolver and some does't implement any
   * resolver. IllegalStateException is expected.
   */
  @Test
  public void testPut_FixedPartitionResolver_NoResolver() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putThroughDataStore_FixedPartitionResolver_NoResolver("Quarter");
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class);
    });
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores. Here No Resolver is provided
   * through attributes. Some keys implement FixedPartitionResolver and some implements
   * PartitionResolver. IllegalStateException is expected.
   */
  @Test
  public void testPut_FixedPartitionResolver_PartitionResolver() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12, null);
    });

    member1.invoke(() -> {
      Throwable thrown = catchThrowable(() -> {
        putThroughDataStore_FixedPartitionResolver_PartitionResolver("Quarter");
      });

      assertThat(thrown)
          .isInstanceOf(IllegalStateException.class);
    });
  }

  /**
   * This tests validate that datastore member tries the put on itself as well as other datastores
   * as per primary FixedPartitionAttributes defined on datastores with only one bucket per
   * partition.
   */
  @Test
  public void testFPR_DefaultNumBuckets() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, true);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, true);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, true);
      createPartitionedRegion("Quarter", singletonList(fpa), 0, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimaryData(QUARTER_1));
    member2.invoke(() -> checkPrimaryData(QUARTER_2));
    member3.invoke(() -> checkPrimaryData(QUARTER_3));
    member4.invoke(() -> checkPrimaryData(QUARTER_4));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(1, 1));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(1, 1));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(1, 1));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(1, 1));
  }

  /**
   * This tests validate that accessor member does the put on datastores as per primary and
   * secondary FixedPartitionAttributes defined on datastores.
   */
  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor() throws ParseException {
    cache.set(new CacheFactory(getDistributedSystemProperties()).create());
    createPartitionedRegion("Quarter", null, 3, 0, 12, newQuarterPartitionResolver());

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    putThroughDataStore("Quarter");

    member1.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_3, false));
    member4.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_4, false));
  }

  /**
   * This tests validate that datastore member does the put on itself as well as other datastores as
   * per primary and secondary FixedPartitionAttributes defined on datastores.
   */
  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimarySecondaryData(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData(QUARTER_3, false));
    member4.invoke(() -> checkPrimarySecondaryData(QUARTER_4, false));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
  }

  /**
   * This tests validate that if only the secondary partitions are available then put should happen
   * successfully for these secondary partitions. These secondary partitions should acts as primary.
   * When the primary partition joins the system then this new member should create the primary
   * buckets for this partition on itself. And Secondary partitions who were holding primary buckets
   * status should now act as secondary buckets.
   */
  @Test
  public void testPut_ValidateDataOnMember_OnlySecondary_Datastore() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_3, false, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", singletonList(fpa), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));

    cache.set(new CacheFactory(getDistributedSystemProperties()).create());
    FixedPartitionAttributes fpa = createFixedPartition(QUARTER_1, true, 3);
    createPartitionedRegion("Quarter", singletonList(fpa), 3, 40, 12,
        newQuarterPartitionResolver());

    Wait.pause(1000);

    member1.invoke(() -> checkPrimaryBucketsForQuarter(3, 0));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(3, 3));
    checkPrimaryBucketsForQuarter(3, 3);
  }

  /**
   * Accessor =1 Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8)
   * Member2 = Q2(9,10,11) Q3(3,4,5), Q4(6,7,8) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11) Member4 =
   * Q4(6,7,8) Q1(0,1,2), Q2(9,10,11) Put happens for all buckets Member 4 goes down, then either
   * member1 or member2 holds primary for member4
   *
   * <p>
   * Primary Secondary Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8) Member2 = Q2(9,10,11), Q4(6,7,8)
   * Q3(3,4,5) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11)
   *
   * <p>
   * Put happens considering Member2 is holding primary for Q4.
   *
   * <p>
   * Member4 comes again, then Member4 should do the GII from member2 for buckets 6,7,8 and should
   * acquire primary status Member1 = Q1(0,1,2) Q3(3,4,5), Q4(6,7,8) Member2 = Q2(9,10,11)
   * Q3(3,4,5), Q4(6,7,8) Member3 = Q3(3,4,5) Q1(0,1,2), Q2(9,10,11) Member4 = Q4(6,7,8) Q1(0,1,2),
   * Q2(9,10,11)
   */
  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Accessor_CacheClosed()
      throws ParseException {
    cache.set(new CacheFactory(getDistributedSystemProperties()).create());
    createPartitionedRegion("Quarter", null, 3, 0, 12, newQuarterPartitionResolver());

    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3), 3, 40, 12,
          newQuarterPartitionResolver());
    });

    putThroughDataStore("Quarter");

    member1.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_3, false));
    member4.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_4, false));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(9, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(9, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(9, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(9, 3));

    member4.invoke(() -> cache.get().close());

    Wait.pause(1000);

    member1.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData_TwoSecondaries(QUARTER_3, false));

    member1.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(9, 3));
  }

  /**
   * Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 = Q2(3,4,5)
   * Q3(6,7,8) Member3 = Q3(6,7,8) Q4(9,10,11) Member4 = Q4(9,10,11) Q1(0,1,2) Put happens for all
   * buckets Member 4 goes down, then either member1 or member2 holds primary for member4
   *
   * <p>
   * Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 = Q2(3,4,5) Q3(6,7,8) Member3 =
   * Q3(6,7,8), Q4(9,10,11)
   *
   * <p>
   * Put happens considering Member3 is holding primary for Q4.
   *
   * <p>
   * Member4 comes again, then Memeber4 should do the GII from member2 for buckets 6,7,8 and should
   * acquire primary status Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5) Member2 =
   * Q2(3,4,5) Q3(6,7,8) Member3 = Q3(6,7,8) Q4(9,10,11) Member4 = Q4(9,10,11) Q1(0,1,2)
   */
  @Test
  public void testPut_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimarySecondaryData(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData(QUARTER_3, false));
    member4.invoke(() -> checkPrimarySecondaryData(QUARTER_4, false));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));

    member4.invoke(() -> cache.get().close());

    Wait.pause(1000);

    member3.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));

    member1.invoke(() -> putHAData("Quarter"));

    member4.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    Wait.pause(1000);

    member1.invoke(() -> checkPrimarySecondaryData(QUARTER_1, true));
    member2.invoke(() -> checkPrimarySecondaryData(QUARTER_2, true));
    member3.invoke(() -> checkPrimarySecondaryData(QUARTER_3, true));
    member4.invoke(() -> checkPrimarySecondaryData(QUARTER_4, true));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
  }

  @Test
  public void test_Bug46619_Put_ValidateDataOnMember_PrimarySecondary_Datastore_CacheClosed() {
    member1.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member3.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member4.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(() -> checkPrimarySecondaryData(QUARTER_1, false));
    member2.invoke(() -> checkPrimarySecondaryData(QUARTER_2, false));
    member3.invoke(() -> checkPrimarySecondaryData(QUARTER_3, false));
    member4.invoke(() -> checkPrimarySecondaryData(QUARTER_4, false));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));

    member4.invoke(() -> cache.get().close());
    member2.invoke(() -> cache.get().close());

    Wait.pause(1000);

    member3.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));
    member1.invoke(() -> checkPrimaryBucketsForQuarterAfterCacheClosed(6, 6));

    member1.invoke(() -> putHAData("Quarter"));

    member4.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_4, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      createCacheOnMember_DisableMovePrimary();
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_3, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2), 1, 40, 12,
          newQuarterPartitionResolver());
    });

    Wait.pause(1000);

    member1.invoke(() -> checkPrimarySecondaryData(QUARTER_1, true));
    member2.invoke(() -> checkPrimarySecondaryData(QUARTER_2, true));
    member3.invoke(() -> checkPrimarySecondaryData(QUARTER_3, true));
    member4.invoke(() -> checkPrimarySecondaryData(QUARTER_4, true));

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 6));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 0));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 6));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 0));

    member4.invoke(this::doRebalance);

    Wait.pause(2000);

    member1.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member2.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member3.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
    member4.invoke(() -> checkPrimaryBucketsForQuarter(6, 3));
  }

  /**
   * Datastore = 4 Datastores Primary Secondary Member1 = Q1(0,1,2) Q2(3,4,5),Q3(6,7,8),Q4(9,10,11)
   * Member2 = Q3(6,7,8) Q1(0,1,2), Q2(3,4,5),Q4(9,10,11) Member3 = Q2(3,4,5),Q4(9,10,11) Q1(0,1,2),
   * Q3(6,7,8)
   *
   * <p>
   * Put happens for all buckets
   *
   * <p>
   * Member 3 goes down, then either member1 or member2 holds primary for member4
   *
   * <p>
   * Primary Secondary Member1 = Q1(0,1,2),Q2(3,4,5) Q3(6,7,8), Q4(9,10,11) Member2 =
   * Q3(6,7,8),Q4(9,10,11) Q1(0,1,2), Q2(3,4,5)
   *
   * <p>
   * Member 3 comes again then it should be same as it was before member 3 went down
   *
   * <p>
   * Member1 = Q1(0,1,2) Q2(3,4,5),Q3(6,7,8),Q4(9,10,11) Member2 = Q3(6,7,8) Q1(0,1,2),
   * Q2(3,4,5),Q4(9,10,11) Member3 = Q2(3,4,5),Q4(9,10,11) Q1(0,1,2), Q3(6,7,8)
   */
  @Test
  public void testPut_ValidateDataOnMember_MultiplePrimaries_Datastore_CacheClosed() {
    member1.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_1, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_2, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_3, false, 3);
      FixedPartitionAttributes fpa4 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3, fpa4), 2, 40, 12,
          newQuarterPartitionResolver());
    });

    member2.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_3, true, 3);
      FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_1, false, 3);
      FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_2, false, 3);
      FixedPartitionAttributes fpa4 = createFixedPartition(QUARTER_4, false, 3);
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3, fpa4), 2, 40, 12,
          newQuarterPartitionResolver());
    });

    FixedPartitionAttributes fpa1 = createFixedPartition(QUARTER_2, true, 3);
    FixedPartitionAttributes fpa2 = createFixedPartition(QUARTER_4, true, 3);
    FixedPartitionAttributes fpa3 = createFixedPartition(QUARTER_1, false, 3);
    FixedPartitionAttributes fpa4 = createFixedPartition(QUARTER_3, false, 3);

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3, fpa4), 2, 40, 12,
          newQuarterPartitionResolver());
    });

    member1.invoke(() -> putThroughDataStore("Quarter"));

    member1.invoke(this::checkStartingBucketIDs);
    member2.invoke(this::checkStartingBucketIDs);
    member3.invoke(this::checkStartingBucketIDs);

    member3.invoke(() -> cache.get().close());

    Wait.pause(1000);

    member1.invoke(this::checkStartingBucketIDs);
    member2.invoke(this::checkStartingBucketIDs);

    member3.invoke(() -> {
      cache.set(new CacheFactory(getDistributedSystemProperties()).create());
      createPartitionedRegion("Quarter", asList(fpa1, fpa2, fpa3, fpa4), 2, 40, 12,
          newQuarterPartitionResolver());
    });

    Wait.pause(3000);

    member1.invoke(this::checkStartingBucketIDs);
    member2.invoke(this::checkStartingBucketIDs);
    member3.invoke(this::checkStartingBucketIDs);
  }

  private void createRegionWithQuarter(String quarter) {
    FixedPartitionAttributes partition = createFixedPartition(quarter, true, 3);
    createPartitionedRegion("Quarter", singletonList(partition), 0, 40, 12,
        newQuarterPartitionResolver());
  }

  private void createCacheOnMember_DisableMovePrimary() {
    System.setProperty(GEMFIRE_PREFIX + "DISABLE_MOVE_PRIMARIES_ON_STARTUP", "true");
    cache.set(new CacheFactory(getDistributedSystemProperties()).create());
  }

  private void createPartitionedRegion(String regionName,
      Iterable<FixedPartitionAttributes> fpaList,
      int redundantCopies,
      int localMaxMemory,
      int totalNumBuckets,
      PartitionResolver<Object, Object> partitionResolver) {
    PartitionAttributesFactory<?, ?> partitionAttributesFactory =
        new PartitionAttributesFactory<>()
            .setLocalMaxMemory(localMaxMemory)
            .setPartitionResolver(partitionResolver)
            .setRedundantCopies(redundantCopies)
            .setTotalNumBuckets(totalNumBuckets);

    if (fpaList != null) {
      for (FixedPartitionAttributes fpa : fpaList) {
        partitionAttributesFactory.addFixedPartitionAttributes(fpa);
      }
    }

    cache.get()
        .<Date, String>createRegionFactory(PARTITION)
        .setPartitionAttributes(partitionAttributesFactory.create())
        .create(regionName);
  }

  private void doRebalance() throws InterruptedException {
    ResourceManager manager = cache.get().getResourceManager();
    RebalanceOperation operation = manager.createRebalanceFactory().start();
    operation.getResults();
  }

  private void putThroughAccessor(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_Accessor month : Months_Accessor.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region.put(date, value);
      }
    }
  }

  private void putThroughDataStore(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region.put(date, value);
      }
    }
  }

  private void putForQuarter(String regionName, String quarters) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    switch (quarters) {
      case "Q1":
        for (Q1_Months month : Q1_Months.values()) {
          for (int i = 1; i < 10; i++) {
            Date date = generateDate(i, month.toString(), "Date");
            String value = month.toString() + i;
            region.put(date, value);
          }
        }
        break;
      case "Q2":
        for (Q2_Months month : Q2_Months.values()) {
          for (int i = 1; i < 10; i++) {
            Date date = generateDate(i, month.toString(), "Date");
            String value = month.toString() + i;
            region.put(date, value);
          }
        }
        break;
      case "Q3":
        for (Q3_Months month : Q3_Months.values()) {
          for (int i = 1; i < 10; i++) {
            Date date = generateDate(i, month.toString(), "Date");
            String value = month.toString() + i;
            region.put(date, value);
          }
        }
        break;
      case "Q4":
        for (Q4_Months month : Q4_Months.values()) {
          for (int i = 1; i < 10; i++) {
            Date date = generateDate(i, month.toString(), "Date");
            String value = month.toString() + i;
            region.put(date, value);
          }
        }
        break;
      default:
        fail("Wrong Quarter");
        break;
    }
  }

  private void getThroughDataStore(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;

        assertThat(region.get(date)).isEqualTo(value);
      }
    }
  }

  private void putThroughDataStore_NoResolver(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region.put(date, value);
      }
    }
  }

  private void putThroughDataStore_CallBackWithResolver(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        Date callbackDate = generateDate(i, month.toString(), "MyDate3");
        String value = month.toString() + i;
        region.put(date, value, callbackDate);
      }
    }
  }

  private void putThroughDataStore_FixedPartitionResolver_NoResolver(String regionName)
      throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 1; i < 10; i++) {
        if (i % 2 == 1) {
          MyDate1 date = (MyDate1) generateDate(i, month.toString(), "MyDate1");
          String value = month.toString() + i;
          region.put(date, value);
        } else {
          Date date = generateDate(i, month.toString(), "Date");
          String value = month.toString() + i;
          region.put(date, value);
        }
      }
    }
  }

  private void putThroughDataStore_FixedPartitionResolver_PartitionResolver(String regionName)
      throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (int i = 1; i < 10; i++) {
      for (Months_DataStore month : Months_DataStore.values()) {
        if (month.ordinal() % 2 == 1) {
          MyDate1 date = (MyDate1) generateDate(i, month.toString(), "MyDate1");
          String value = month.toString() + i;
          region.put(date, value);
        } else {
          MyDate2 date = (MyDate2) generateDate(i, month.toString(), "MyDate2");
          String value = month.toString() + i;
          region.put(date, value);
        }
      }
    }
  }

  private void deleteOperation(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);
    Date date = generateDate(1, "JAN", "Date");
    region.destroy(date);
  }

  private void putHAData(String regionName) throws ParseException {
    Region<Date, String> region = cache.get().getRegion(regionName);

    for (Months_DataStore month : Months_DataStore.values()) {
      for (int i = 10; i < 20; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        String value = month.toString() + i;
        region.put(date, value);
      }
    }
  }

  private Date generateDate(int i, String month, String dateType) throws ParseException {
    String day = i > 0 && i < 10 ? "0" + i : Integer.toString(i);
    String dateString = day + "-" + month + "-2010";
    SimpleDateFormat sdf = new SimpleDateFormat("dd-MMM-yyyy", Locale.US);

    if (StringUtils.equals(dateType, "Date")) {
      return sdf.parse(dateString);
    }
    if (StringUtils.equals(dateType, "MyDate1")) {
      return new MyDate1(sdf.parse(dateString).getTime());
    }
    if (StringUtils.equals(dateType, "MyDate2")) {
      return new MyDate2(sdf.parse(dateString).getTime());
    }
    if (StringUtils.equals(dateType, "MyDate3")) {
      return new MyDate3(sdf.parse(dateString).getTime());
    }
    return null;
  }

  private void checkPrimaryData(String partitionName) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    switch (partitionName) {
      case QUARTER_1:
        assertThat(localRegion.size()).isEqualTo(27);
        assertTRUE_Q1(false);
        assertFALSE_Q2(false);
        assertFALSE_Q3(false);
        assertFALSE_Q4(false);
        break;
      case QUARTER_2:
        assertThat(localRegion.size()).isEqualTo(27);
        assertFALSE_Q1(false);
        assertTRUE_Q2(false);
        assertFALSE_Q3(false);
        assertFALSE_Q4(false);
        break;
      case QUARTER_3:
        assertThat(localRegion.size()).isEqualTo(27);
        assertFALSE_Q1(false);
        assertFALSE_Q2(false);
        assertTRUE_Q3(false);
        assertFALSE_Q4(false);
        break;
      case QUARTER_4:
        assertThat(localRegion.size()).isEqualTo(27);
        assertFALSE_Q1(false);
        assertFALSE_Q2(false);
        assertFALSE_Q3(false);
        assertTRUE_Q4(false);
        break;
    }
  }

  private void checkPrimarySecondaryData(String partitionName, boolean isHA) throws ParseException {
    switch (partitionName) {
      case QUARTER_1:
        assertTRUE_Q1(isHA);
        assertTRUE_Q2(isHA);
        assertFALSE_Q3(isHA);
        assertFALSE_Q4(isHA);
        break;
      case QUARTER_2:
        assertFALSE_Q1(isHA);
        assertTRUE_Q2(isHA);
        assertTRUE_Q3(isHA);
        assertFALSE_Q4(isHA);
        break;
      case QUARTER_3:
        assertFALSE_Q1(isHA);
        assertFALSE_Q2(isHA);
        assertTRUE_Q3(isHA);
        assertTRUE_Q4(isHA);
        break;
      case QUARTER_4:
        assertTRUE_Q1(isHA);
        assertFALSE_Q2(isHA);
        assertFALSE_Q3(isHA);
        assertTRUE_Q4(isHA);
        break;
    }
  }

  private void checkPrimarySecondaryData_TwoSecondaries(String partitionName, boolean isHA)
      throws ParseException {
    switch (partitionName) {
      case QUARTER_1:
        assertTRUE_Q1(isHA);
        assertFALSE_Q2(isHA);
        assertTRUE_Q3(isHA);
        assertTRUE_Q4(isHA);
        break;
      case QUARTER_2:
        assertFALSE_Q1(isHA);
        assertTRUE_Q2(isHA);
        assertTRUE_Q3(isHA);
        assertTRUE_Q4(isHA);
        break;
      case QUARTER_3:
        assertTRUE_Q1(isHA);
        assertTRUE_Q2(isHA);
        assertTRUE_Q3(isHA);
        assertFALSE_Q4(isHA);
        break;
      case QUARTER_4:
        assertTRUE_Q1(isHA);
        assertTRUE_Q2(isHA);
        assertFALSE_Q3(isHA);
        assertTRUE_Q4(isHA);
        break;
    }
  }

  private void assertTRUE_Q1(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q1_Months month : Q1_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isTrue();
      }
    }
  }

  private void assertTRUE_Q2(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q2_Months month : Q2_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isTrue();
      }
    }
  }

  private void assertTRUE_Q3(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q3_Months month : Q3_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isTrue();
      }
    }
  }

  private void assertTRUE_Q4(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q4_Months month : Q4_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isTrue();
      }
    }
  }

  private void assertFALSE_Q1(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q1_Months month : Q1_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isFalse();
      }
    }
  }

  private void assertFALSE_Q2(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q2_Months month : Q2_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isFalse();
      }
    }
  }

  private void assertFALSE_Q3(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q3_Months month : Q3_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isFalse();
      }
    }
  }

  private void assertFALSE_Q4(boolean isHA) throws ParseException {
    Region localRegion = getLocalData(cache.get().getRegion(REGION_NAME));

    int day = isHA ? 20 : 10;

    for (Q4_Months month : Q4_Months.values()) {
      for (int i = 1; i < day; i++) {
        Date date = generateDate(i, month.toString(), "Date");
        assertThat(localRegion.keySet().contains(date)).isFalse();
      }
    }
  }

  private void checkPrimaryBucketsForQuarter(int numBuckets, int primaryBuckets) {
    PartitionedRegionDataStore dataStore = getDataStore(REGION_NAME);

    assertThat(dataStore.getSizeLocally())
        .hasSize(numBuckets);
    assertThat(dataStore.getNumberOfPrimaryBucketsManaged())
        .isEqualTo(primaryBuckets);
  }

  private void checkPrimaryBucketsForQuarterAfterCacheClosed(int numBuckets, int primaryBuckets) {
    PartitionedRegionDataStore dataStore = getDataStore(REGION_NAME);

    assertThat(dataStore.getSizeLocally())
        .hasSize(numBuckets);
    assertThat(dataStore.getNumberOfPrimaryBucketsManaged() % primaryBuckets)
        .isZero();
  }

  private void checkStartingBucketIDs() {
    PartitionedRegionDataStore dataStore = getDataStore(REGION_NAME);
    assertThat(dataStore.getAllLocalPrimaryBucketIds().size() % 3)
        .isZero();
  }

  private void setPRObserverBeforeCalculateStartingBucketId() {
    PartitionedRegion.BEFORE_CALCULATE_STARTING_BUCKET_FLAG = true;

    PartitionedRegionObserverHolder.setInstance(new PartitionedRegionObserverAdapter() {
      @Override
      public void beforeCalculatingStartingBucketId() {
        blackboard.signalGate("waiting");
        try {
          blackboard.waitForGate("done");
        } catch (TimeoutException | InterruptedException e) {
          errorCollector.addError(e);
          throw new RuntimeException(e);
        }
      }
    });
  }

  private PartitionedRegionDataStore getDataStore(String regionName) {
    Region<Date, String> region = cache.get().getRegion(regionName);
    PartitionedRegion partitionedRegion = (PartitionedRegion) region;
    return partitionedRegion.getDataStore();
  }

  private PartitionResolver<Object, Object> newQuarterPartitionResolver() {
    return new QuarterPartitionResolver<>();
  }

  private static int getZeroBasedMonth(Date date) {
    return Instant.ofEpochMilli(date.getTime())
        .atZone(ZoneId.systemDefault())
        .toLocalDate()
        .getMonthValue() - 1;
  }

  private static String getQuarterFromZeroBasedMonth(int month) {
    switch (month) {
      case 0:
      case 1:
      case 2:
        return "Q1";
      case 3:
      case 4:
      case 5:
        return "Q2";
      case 6:
      case 7:
      case 8:
        return "Q3";
      case 9:
      case 10:
      case 11:
        return "Q4";
      default:
        return "Invalid Quarter";
    }
  }

  public enum Months_Accessor {
    JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP
  }

  public enum Months_DataStore {
    JAN, FEB, MAR, APR, MAY, JUN, JUL, AUG, SEP, OCT, NOV, DEC
  }

  public enum Q1_Months {
    JAN, FEB, MAR
  }

  public enum Q2_Months {
    APR, MAY, JUN
  }

  public enum Q3_Months {
    JUL, AUG, SEP
  }

  public enum Q4_Months {
    OCT, NOV, DEC
  }

  public static class QuarterPartitionResolver<K, V>
      implements FixedPartitionResolver<K, V>, Declarable2, DataSerializable {

    private final Properties resolveProps;

    public QuarterPartitionResolver() {
      resolveProps = new Properties();
      resolveProps.setProperty("routingType", "key");
    }

    @Override
    public String getName() {
      return "QuarterPartitionResolver";
    }

    @Override
    public String getPartitionName(EntryOperation opDetails, Set allAvailablePartitions) {
      int month = getZeroBasedMonth((Date) opDetails.getKey());
      return getQuarterFromZeroBasedMonth(month);
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return getZeroBasedMonth((Date) opDetails.getKey());
    }

    @Override
    public Properties getConfig() {
      return resolveProps;
    }

    @Override
    public void init(Properties props) {
      resolveProps.putAll(props);
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeProperties(resolveProps, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      Properties inProps = DataSerializer.readProperties(in);
      resolveProps.putAll(inProps);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(resolveProps);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!obj.getClass().equals(getClass())) {
        return false;
      }
      QuarterPartitionResolver other = (QuarterPartitionResolver) obj;
      return resolveProps.equals(other.getConfig());
    }
  }

  public static class MyDate1 extends Date implements FixedPartitionResolver {

    public MyDate1(long time) {
      super(time);
    }

    @Override
    public String getName() {
      return "MyDate1";
    }

    @Override
    public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
      int month = getZeroBasedMonth((Date) opDetails.getKey());
      return getQuarterFromZeroBasedMonth(month);
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return getZeroBasedMonth((Date) opDetails.getKey());
    }
  }

  public static class MyDate2 extends Date implements PartitionResolver {

    public MyDate2(long time) {
      super(time);
    }

    @Override
    public String getName() {
      return "MyDate2";
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return getZeroBasedMonth((Date) opDetails.getKey());
    }

    public String toString() {
      return "MyDate2";
    }
  }

  public static class MyDate3 extends Date implements FixedPartitionResolver {

    public MyDate3(long time) {
      super(time);
    }

    @Override
    public String getName() {
      return "MyDate3";
    }

    @Override
    public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
      int month = getZeroBasedMonth((Date) opDetails.getKey());
      return getQuarterFromZeroBasedMonth(month);
    }

    @Override
    public Serializable getRoutingObject(EntryOperation opDetails) {
      return getZeroBasedMonth((Date) opDetails.getKey());
    }
  }
}
