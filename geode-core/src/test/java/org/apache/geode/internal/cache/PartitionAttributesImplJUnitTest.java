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
package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.test.fake.Fakes;

/**
 * Unit test suite for PartitionAttributesImpl.
 */
public class PartitionAttributesImplJUnitTest {

  private String colocatedRegionFullPath;

  private Properties globalProps;
  private String globalProps_key1;
  private String globalProps_value1;

  private Properties localProps;
  private String localProps_key1;
  private String localProps_value1;

  private int localMaxMemory;
  private boolean offHeap;
  private PartitionResolver<Object, Object> partitionResolver;

  private long recoveryDelay;
  private int redundancy;
  private long startupRecoveryDelay;
  private long totalMaxMemory;
  private int maxNumberOfBuckets;

  private int newTestAvailableOffHeapMemory;
  private int greaterLocalMaxMemory;

  private FixedPartitionAttributesImpl fixedPartitionAttributes;
  private PartitionListener partitionListener;
  private Cache cache;

  @Before
  public void before() {
    colocatedRegionFullPath = "colocatedRegionFullPath";

    globalProps = new Properties();
    globalProps_key1 = "globalProps_key1";
    globalProps_value1 = "globalProps_value1";
    globalProps.setProperty(globalProps_key1, globalProps_value1);

    localProps = new Properties();
    localProps_key1 = "localProps_key1";
    localProps_value1 = "localProps_value1";
    localProps.setProperty(localProps_key1, localProps_value1);

    localMaxMemory = 123;
    offHeap = false;
    cache = Fakes.cache();

    partitionResolver = new PartitionResolver<Object, Object>() {
      @Override
      public void close() {}

      @Override
      public Object getRoutingObject(EntryOperation opDetails) {
        return "partitionResolver_getRoutingObject";
      }

      @Override
      public String getName() {
        return "partitionResolver_getName";
      }
    };

    recoveryDelay = 234;
    redundancy = 345;
    startupRecoveryDelay = 456;
    totalMaxMemory = 567;
    maxNumberOfBuckets = 678;

    newTestAvailableOffHeapMemory = 789;
    greaterLocalMaxMemory = 890;

    partitionListener = new PartitionListener() {
      @Override
      public void afterPrimary(int bucketId) {}

      @Override
      public void afterRegionCreate(Region<?, ?> region) {}

      @Override
      public void afterBucketRemoved(int bucketId, Iterable<?> keys) {}

      @Override
      public void afterBucketCreated(int bucketId, Iterable<?> keys) {}
    };
  }

  @Test
  public void testSetters() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(localMaxMemory);

    assertEquals(colocatedRegionFullPath, instance.getColocatedWith());
    assertEquals(globalProps, instance.getGlobalProperties());
    assertEquals(localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(localProps, instance.getLocalProperties());
    assertEquals(offHeap, instance.getOffHeap());
    assertEquals(partitionResolver, instance.getPartitionResolver());
    assertEquals(recoveryDelay, instance.getRecoveryDelay());
    assertEquals(redundancy, instance.getRedundancy());
    assertEquals(startupRecoveryDelay, instance.getStartupRecoveryDelay());
    assertEquals(totalMaxMemory, instance.getTotalMaxMemory());
    assertEquals(maxNumberOfBuckets, instance.getTotalNumBuckets());
  }

  @Test
  public void testMergeWithoutOffHeap() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(localMaxMemory);

    PartitionAttributesImpl destination = new PartitionAttributesImpl();
    destination.merge(instance);

    assertEquals(colocatedRegionFullPath, destination.getColocatedWith());
    // assertIndexDetailsEquals(this.globalProps, destination.getGlobalProperties());
    // assertIndexDetailsEquals(this.localProps, destination.getLocalProperties());
    assertEquals(partitionResolver, destination.getPartitionResolver());
    assertEquals(recoveryDelay, destination.getRecoveryDelay());
    assertEquals(redundancy, destination.getRedundancy());
    assertEquals(startupRecoveryDelay, destination.getStartupRecoveryDelay());
    assertEquals(totalMaxMemory, destination.getTotalMaxMemory());
    assertEquals(maxNumberOfBuckets, destination.getTotalNumBuckets());
  }

  @Test
  public void testCloneWithoutOffHeap() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(localMaxMemory);

    PartitionAttributesImpl clone = (PartitionAttributesImpl) instance.clone();
    assertGetValues(clone);
    assertEquals(localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(newTestAvailableOffHeapMemory + "m");
    offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();

    PartitionAttributesImpl clone = (PartitionAttributesImpl) instance.clone();
    assertGetValues(clone);
    assertEquals(newTestAvailableOffHeapMemory, instance.getLocalMaxMemory());
    assertEquals(offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndLesserLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(newTestAvailableOffHeapMemory + "m");
    offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(localMaxMemory);

    PartitionAttributesImpl clone = (PartitionAttributesImpl) instance.clone();
    assertGetValues(clone);
    assertEquals(localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndGreaterLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(newTestAvailableOffHeapMemory + "m");
    offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(greaterLocalMaxMemory);

    PartitionAttributesImpl clone = (PartitionAttributesImpl) instance.clone();
    assertGetValues(clone);
    assertEquals(greaterLocalMaxMemory, instance.getLocalMaxMemory());
    assertEquals(offHeap, instance.getOffHeap());
  }

  @Test
  public void testLocalPropertiesWithLOCAL_MAX_MEMORY_PROPERTY() {
    int value = 111;

    Properties props = new Properties();
    props.setProperty(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY, String.valueOf(value));

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setLocalProperties(props);

    assertNotNull(instance.getLocalProperties());
    assertFalse(instance.getLocalProperties().isEmpty());
    assertEquals(props, instance.getLocalProperties());
    assertEquals(value, instance.getLocalMaxMemory());
  }

  @Test
  public void testLocalPropertiesWithoutLOCAL_MAX_MEMORY_PROPERTY() {
    int value = 111;

    Properties props = new Properties();

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setLocalProperties(props);

    assertNotNull(instance.getLocalProperties());
    assertTrue(instance.getLocalProperties().isEmpty());
    assertEquals(props, instance.getLocalProperties());
    assertEquals(instance.getLocalMaxMemoryDefault(), instance.getLocalMaxMemory());
  }

  @Test
  public void testGlobalPropertiesWithGLOBAL_MAX_MEMORY_PROPERTY() {
    int value = 777;

    Properties props = new Properties();
    props.setProperty(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY, String.valueOf(value));

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setGlobalProperties(props);

    assertNotNull(instance.getGlobalProperties());
    assertFalse(instance.getGlobalProperties().isEmpty());
    assertEquals(props, instance.getGlobalProperties());
    assertEquals(value, instance.getTotalMaxMemory());
  }

  @Test
  public void testGlobalPropertiesWithoutGLOBAL_MAX_MEMORY_PROPERTY() {
    int value = 777;

    Properties props = new Properties();

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setGlobalProperties(props);

    assertNotNull(instance.getGlobalProperties());
    assertTrue(instance.getGlobalProperties().isEmpty());
    assertEquals(props, instance.getGlobalProperties());
    assertEquals(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT,
        instance.getTotalMaxMemory());
  }

  @Test
  public void testGetLocalMaxMemoryDefault() {
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    assertEquals(PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT,
        instance.getLocalMaxMemoryDefault());
  }

  @Test
  public void testSetAllWithOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(colocatedRegionFullPath);

    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(fixedPartitionAttributes);

    // with Default LocalMaxMemory
    // source.setLocalMaxMemory(this.localMaxMemory);

    // with OffHeap
    offHeap = true;
    source.setOffHeap(offHeap);

    source.setPartitionResolver(partitionResolver);
    source.setRecoveryDelay(recoveryDelay);
    source.setRedundantCopies(redundancy);
    source.setStartupRecoveryDelay(startupRecoveryDelay);
    source.setTotalMaxMemory(totalMaxMemory);
    source.setTotalNumBuckets(maxNumberOfBuckets);

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);

    assertEquals(source.getLocalMaxMemory(), instance.getLocalMaxMemory());
    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }

  @Test
  public void testSetAllWithoutOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(colocatedRegionFullPath);

    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(fixedPartitionAttributes);

    // with Default LocalMaxMemory
    // source.setLocalMaxMemory(this.localMaxMemory);

    // without OffHeap
    // this.offHeap = true;
    // source.setOffHeap(this.offHeap);

    source.setPartitionResolver(partitionResolver);
    source.setRecoveryDelay(recoveryDelay);
    source.setRedundantCopies(redundancy);
    source.setStartupRecoveryDelay(startupRecoveryDelay);
    source.setTotalMaxMemory(totalMaxMemory);
    source.setTotalNumBuckets(maxNumberOfBuckets);

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);

    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }

  @Test
  public void testSetAllWithoutOffHeapAndNonDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(colocatedRegionFullPath);

    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(fixedPartitionAttributes);

    // with NonDefault LocalMaxMemory
    source.setLocalMaxMemory(localMaxMemory);

    // without OffHeap
    // this.offHeap = true;
    // source.setOffHeap(this.offHeap);

    source.setPartitionResolver(partitionResolver);
    source.setRecoveryDelay(recoveryDelay);
    source.setRedundantCopies(redundancy);
    source.setStartupRecoveryDelay(startupRecoveryDelay);
    source.setTotalMaxMemory(totalMaxMemory);
    source.setTotalNumBuckets(maxNumberOfBuckets);

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);

    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }

  @Test
  public void testSetAllWithOffHeapAndNonDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(colocatedRegionFullPath);

    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(fixedPartitionAttributes);

    // with NonDefault LocalMaxMemory
    source.setLocalMaxMemory(localMaxMemory);

    // with OffHeap
    offHeap = true;
    source.setOffHeap(offHeap);

    source.setPartitionResolver(partitionResolver);
    source.setRecoveryDelay(recoveryDelay);
    source.setRedundantCopies(redundancy);
    source.setStartupRecoveryDelay(startupRecoveryDelay);
    source.setTotalMaxMemory(totalMaxMemory);
    source.setTotalNumBuckets(maxNumberOfBuckets);

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);

    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }

  @Test
  public void testSetAllWithLocalAndGlobalProperties() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    fillInForSetAllWithPropertiesTest(source);

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);

    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }

  @Test
  public void testFillInForSetAllWithPropertiesTestAndHashCode() {
    PartitionAttributesImpl one = new PartitionAttributesImpl();
    fillInForSetAllWithPropertiesTest(one);

    PartitionAttributesImpl two = new PartitionAttributesImpl();
    fillInForSetAllWithPropertiesTest(two);

    assertEquals(one.hashCode(), two.hashCode());
    assertEquals(one, two);
  }

  @Test
  @Ignore
  public void testAddFixedPartitionAttributes() {

  }

  @Test
  @Ignore
  public void testAddPartitionListener() {

  }

  @Test
  @Ignore
  public void testAddPartitionListeners() {

  }

  @Test
  public void testEqualsAndHashCodeForEqualInstances() {
    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    fillInForEqualityTest(instance);
    instance.addFixedPartitionAttributes(fixedPartitionAttributes);

    PartitionAttributesImpl other = new PartitionAttributesImpl();
    fillInForEqualityTest(other);
    other.addFixedPartitionAttributes(fixedPartitionAttributes);

    assertEquals(instance.hashCode(), other.hashCode());
    assertEquals(instance, other);
  }

  @Test
  public void testEqualsForNonEqualInstances() {
    FixedPartitionAttributesImpl fixedPartitionAttributes1 = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes1.setPartitionName("setPartitionName1");

    FixedPartitionAttributesImpl fixedPartitionAttributes2 = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes2.setPartitionName("setPartitionName2");

    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    fillInForEqualityTest(instance);
    instance.addFixedPartitionAttributes(fixedPartitionAttributes1);

    PartitionAttributesImpl other = new PartitionAttributesImpl();
    fillInForEqualityTest(other);
    other.addFixedPartitionAttributes(fixedPartitionAttributes2);

    assertNotEquals(instance, other);
  }

  @Test
  public void validateColocationWithNonExistingRegion() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setColocatedWith("nonExistingRegion");
    assertThatThrownBy(() -> instance.validateColocation(cache))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("It should be created before setting");
  }

  @Test
  public void validateColocationWithNonPartitionedRegion() {
    Region region = mock(Region.class);
    when(cache.getRegion("nonPrRegion")).thenReturn(region);
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setColocatedWith("nonPrRegion");

    assertThatThrownBy(() -> instance.validateColocation(cache))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("supported only for PartitionedRegions");
  }

  @Test
  public void validateColocationWithSimilarPartitionedRegion() {
    PartitionedRegion region = mock(PartitionedRegion.class);
    PartitionAttributes prAttributes = mock(PartitionAttributes.class);
    when(cache.getRegion("PrRegion")).thenReturn(region);
    when(region.getPartitionAttributes()).thenReturn(prAttributes);

    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    when(prAttributes.getTotalNumBuckets()).thenReturn(instance.getTotalNumBuckets());
    when(prAttributes.getRedundantCopies()).thenReturn(instance.getRedundantCopies());
    instance.setColocatedWith("PrRegion");
    instance.validateColocation(cache);
    verify(cache, times(1)).getRegion("PrRegion");
  }

  private void fillInForEqualityTest(PartitionAttributesImpl instance) {
    instance.setRedundantCopies(redundancy);
    instance.setLocalMaxMemory(localMaxMemory);
    instance.setTotalMaxMemory(totalMaxMemory);
    instance.setTotalNumBuckets(maxNumberOfBuckets);
    instance.setStartupRecoveryDelay(startupRecoveryDelay);
    instance.setRecoveryDelay(recoveryDelay);
    instance.setPartitionResolver(partitionResolver);
    instance.setColocatedWith(colocatedRegionFullPath);
    instance.addFixedPartitionAttributes(fixedPartitionAttributes);
    instance.addPartitionListener(partitionListener);
  }

  private void assertGetValues(final PartitionAttributesImpl instance) {
    assertEquals(colocatedRegionFullPath, instance.getColocatedWith());
    assertEquals(globalProps, instance.getGlobalProperties());
    assertEquals(localProps, instance.getLocalProperties());
    assertEquals(partitionResolver, instance.getPartitionResolver());
    assertEquals(recoveryDelay, instance.getRecoveryDelay());
    assertEquals(redundancy, instance.getRedundancy());
    assertEquals(startupRecoveryDelay, instance.getStartupRecoveryDelay());
    assertEquals(totalMaxMemory, instance.getTotalMaxMemory());
    assertEquals(maxNumberOfBuckets, instance.getTotalNumBuckets());
  }

  private PartitionAttributesImpl createPartitionAttributesImpl() {
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setColocatedWith(colocatedRegionFullPath);
    instance.setGlobalProperties(globalProps);
    instance.setLocalProperties(localProps);
    instance.setOffHeap(offHeap);
    instance.setPartitionResolver(partitionResolver);
    instance.setRecoveryDelay(recoveryDelay);
    instance.setRedundantCopies(redundancy);
    instance.setStartupRecoveryDelay(startupRecoveryDelay);
    instance.setTotalMaxMemory(totalMaxMemory);
    instance.setTotalNumBuckets(maxNumberOfBuckets);
    return instance;
  }

  private void fillInForSetAllWithPropertiesTest(PartitionAttributesImpl instance) {
    instance.setColocatedWith(colocatedRegionFullPath);

    instance.setLocalProperties(localProps);
    instance.setGlobalProperties(globalProps);

    fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    fixedPartitionAttributes.setPartitionName("setPartitionName");
    instance.addFixedPartitionAttributes(fixedPartitionAttributes);

    instance.setLocalMaxMemory(localMaxMemory);

    offHeap = true;
    instance.setOffHeap(offHeap);

    // instance.addPartitionListener(this.partitionListener);
    instance.setPartitionResolver(partitionResolver);
    instance.setRecoveryDelay(recoveryDelay);
    instance.setRedundantCopies(redundancy);
    instance.setStartupRecoveryDelay(startupRecoveryDelay);
    instance.setTotalMaxMemory(totalMaxMemory);
    instance.setTotalNumBuckets(maxNumberOfBuckets);
  }
}
