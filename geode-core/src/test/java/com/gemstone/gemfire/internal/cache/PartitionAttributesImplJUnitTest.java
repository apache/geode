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

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListener;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Unit test suite for PartitionAttributesImpl.
 */
@Category(UnitTest.class)
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
  
  @Before
  public void before() {
    this.colocatedRegionFullPath = "colocatedRegionFullPath";
    
    this.globalProps = new Properties();
    this.globalProps_key1 = "globalProps_key1";
    this.globalProps_value1 = "globalProps_value1";
    this.globalProps.setProperty(globalProps_key1, this.globalProps_value1);

    this.localProps = new Properties();
    this.localProps_key1 = "localProps_key1";
    this.localProps_value1 = "localProps_value1";
    this.localProps.setProperty(localProps_key1, this.localProps_value1);

    this.localMaxMemory = 123;
    this.offHeap = false;
    
    this.partitionResolver = new PartitionResolver<Object, Object>() {
      @Override
      public void close() {
      }
      @Override
      public Object getRoutingObject(EntryOperation opDetails) {
        return "partitionResolver_getRoutingObject";
      }
      @Override
      public String getName() {
        return "partitionResolver_getName";
      }
    };
    
    this.recoveryDelay = 234;
    this.redundancy = 345;
    this.startupRecoveryDelay = 456;
    this.totalMaxMemory = 567;
    this.maxNumberOfBuckets = 678;
    
    this.newTestAvailableOffHeapMemory = 789;
    this.greaterLocalMaxMemory = 890;
    
    this.partitionListener = new PartitionListener() {
      @Override
      public void afterPrimary(int bucketId) {
      }
      @Override
      public void afterRegionCreate(Region<?, ?> region) {
      }

      @Override
      public void afterBucketRemoved(int bucketId, Iterable<?> keys) {
      }
      @Override
      public void afterBucketCreated(int bucketId, Iterable<?> keys) {
      }
    };
  }
  
  @Test
  public void testSetters() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(this.localMaxMemory);
    
    assertEquals(this.colocatedRegionFullPath, instance.getColocatedWith());
    assertEquals(this.globalProps, instance.getGlobalProperties());
    assertEquals(this.localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(this.localProps, instance.getLocalProperties());
    assertEquals(this.offHeap, instance.getOffHeap());
    assertEquals(this.partitionResolver, instance.getPartitionResolver());
    assertEquals(this.recoveryDelay, instance.getRecoveryDelay());
    assertEquals(this.redundancy, instance.getRedundancy());
    assertEquals(this.startupRecoveryDelay, instance.getStartupRecoveryDelay());
    assertEquals(this.totalMaxMemory, instance.getTotalMaxMemory());
    assertEquals(this.maxNumberOfBuckets, instance.getTotalNumBuckets());
  }
  
  @Test
  public void testMergeWithoutOffHeap() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(this.localMaxMemory);
    
    PartitionAttributesImpl destination = new PartitionAttributesImpl();
    destination.merge(instance);
    
    assertEquals(this.colocatedRegionFullPath, destination.getColocatedWith());
    //assertIndexDetailsEquals(this.globalProps, destination.getGlobalProperties());
    //assertIndexDetailsEquals(this.localProps, destination.getLocalProperties());
    assertEquals(this.partitionResolver, destination.getPartitionResolver());
    assertEquals(this.recoveryDelay, destination.getRecoveryDelay());
    assertEquals(this.redundancy, destination.getRedundancy());
    assertEquals(this.startupRecoveryDelay, destination.getStartupRecoveryDelay());
    assertEquals(this.totalMaxMemory, destination.getTotalMaxMemory());
    assertEquals(this.maxNumberOfBuckets, destination.getTotalNumBuckets());
  }
  
  @Test
  public void testCloneWithoutOffHeap() {
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(this.localMaxMemory);
    
    PartitionAttributesImpl clone = (PartitionAttributesImpl)instance.clone();
    assertGetValues(clone);
    assertEquals(this.localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(this.offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(this.newTestAvailableOffHeapMemory+"m");
    this.offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    
    PartitionAttributesImpl clone = (PartitionAttributesImpl)instance.clone();
    assertGetValues(clone);
    assertEquals(this.newTestAvailableOffHeapMemory, instance.getLocalMaxMemory());
    assertEquals(this.offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndLesserLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(this.newTestAvailableOffHeapMemory+"m");
    this.offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(this.localMaxMemory);
    
    PartitionAttributesImpl clone = (PartitionAttributesImpl)instance.clone();
    assertGetValues(clone);
    assertEquals(this.localMaxMemory, instance.getLocalMaxMemory());
    assertEquals(this.offHeap, instance.getOffHeap());
  }

  @Test
  public void testCloneWithOffHeapAndGreaterLocalMaxMemory() {
    PartitionAttributesImpl.setTestAvailableOffHeapMemory(this.newTestAvailableOffHeapMemory+"m");
    this.offHeap = true;
    PartitionAttributesImpl instance = createPartitionAttributesImpl();
    instance.setLocalMaxMemory(this.greaterLocalMaxMemory);
    
    PartitionAttributesImpl clone = (PartitionAttributesImpl)instance.clone();
    assertGetValues(clone);
    assertEquals(this.greaterLocalMaxMemory, instance.getLocalMaxMemory());
    assertEquals(this.offHeap, instance.getOffHeap());
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
    assertEquals(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT, instance.getTotalMaxMemory());
  }
  
  @Test
  public void testGetLocalMaxMemoryDefault() {
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    assertEquals(PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT, instance.getLocalMaxMemoryDefault());
  }
  
  @Test
  public void testSetAllWithOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(this.colocatedRegionFullPath);
    
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    // with Default LocalMaxMemory
    //source.setLocalMaxMemory(this.localMaxMemory);
    
    // with OffHeap
    this.offHeap = true;
    source.setOffHeap(this.offHeap);
    
    source.setPartitionResolver(this.partitionResolver);
    source.setRecoveryDelay(this.recoveryDelay);
    source.setRedundantCopies(this.redundancy);
    source.setStartupRecoveryDelay(this.startupRecoveryDelay);
    source.setTotalMaxMemory(this.totalMaxMemory);
    source.setTotalNumBuckets(this.maxNumberOfBuckets);
    
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);
    
    assertEquals(source.getLocalMaxMemory(), instance.getLocalMaxMemory());
    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }
  
  @Test
  public void testSetAllWithoutOffHeapAndDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(this.colocatedRegionFullPath);
    
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    // with Default LocalMaxMemory
    //source.setLocalMaxMemory(this.localMaxMemory);
    
    // without OffHeap
    //this.offHeap = true;
    //source.setOffHeap(this.offHeap);
    
    source.setPartitionResolver(this.partitionResolver);
    source.setRecoveryDelay(this.recoveryDelay);
    source.setRedundantCopies(this.redundancy);
    source.setStartupRecoveryDelay(this.startupRecoveryDelay);
    source.setTotalMaxMemory(this.totalMaxMemory);
    source.setTotalNumBuckets(this.maxNumberOfBuckets);
    
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);
    
    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }
  
  @Test
  public void testSetAllWithoutOffHeapAndNonDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(this.colocatedRegionFullPath);
    
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    // with NonDefault LocalMaxMemory
    source.setLocalMaxMemory(this.localMaxMemory);
    
    // without OffHeap
    //this.offHeap = true;
    //source.setOffHeap(this.offHeap);
    
    source.setPartitionResolver(this.partitionResolver);
    source.setRecoveryDelay(this.recoveryDelay);
    source.setRedundantCopies(this.redundancy);
    source.setStartupRecoveryDelay(this.startupRecoveryDelay);
    source.setTotalMaxMemory(this.totalMaxMemory);
    source.setTotalNumBuckets(this.maxNumberOfBuckets);
    
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setAll(source);
    
    assertEquals(source, instance);
    assertEquals(source.hashCode(), instance.hashCode());
  }
  
  @Test
  public void testSetAllWithOffHeapAndNonDefaultLocalMaxMemory() {
    PartitionAttributesImpl source = new PartitionAttributesImpl();
    source.setColocatedWith(this.colocatedRegionFullPath);
    
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    source.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    // with NonDefault LocalMaxMemory
    source.setLocalMaxMemory(this.localMaxMemory);
    
    // with OffHeap
    this.offHeap = true;
    source.setOffHeap(this.offHeap);
    
    source.setPartitionResolver(this.partitionResolver);
    source.setRecoveryDelay(this.recoveryDelay);
    source.setRedundantCopies(this.redundancy);
    source.setStartupRecoveryDelay(this.startupRecoveryDelay);
    source.setTotalMaxMemory(this.totalMaxMemory);
    source.setTotalNumBuckets(this.maxNumberOfBuckets);
    
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
  
  @Test @Ignore
  public void testAddFixedPartitionAttributes() {
    
  }
  
  @Test @Ignore
  public void testAddPartitionListener() {
    
  }
  
  @Test @Ignore
  public void testAddPartitionListeners() {
    
  }
  
  @Test
  public void testEqualsAndHashCodeForEqualInstances() {
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    fillInForEqualityTest(instance);
    instance.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    PartitionAttributesImpl other = new PartitionAttributesImpl();
    fillInForEqualityTest(other);
    other.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
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
  
  private void fillInForEqualityTest(PartitionAttributesImpl instance) {
    instance.setRedundantCopies(this.redundancy);
    instance.setLocalMaxMemory(this.localMaxMemory);
    instance.setTotalMaxMemory(this.totalMaxMemory);
    instance.setTotalNumBuckets(this.maxNumberOfBuckets);
    instance.setStartupRecoveryDelay(this.startupRecoveryDelay);
    instance.setRecoveryDelay(this.recoveryDelay);
    instance.setPartitionResolver(this.partitionResolver);
    instance.setColocatedWith(this.colocatedRegionFullPath);
    instance.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    instance.addPartitionListener(this.partitionListener);
  }

  private void assertGetValues(final PartitionAttributesImpl instance) {
    assertEquals(this.colocatedRegionFullPath, instance.getColocatedWith());
    assertEquals(this.globalProps, instance.getGlobalProperties());
    assertEquals(this.localProps, instance.getLocalProperties());
    assertEquals(this.partitionResolver, instance.getPartitionResolver());
    assertEquals(this.recoveryDelay, instance.getRecoveryDelay());
    assertEquals(this.redundancy, instance.getRedundancy());
    assertEquals(this.startupRecoveryDelay, instance.getStartupRecoveryDelay());
    assertEquals(this.totalMaxMemory, instance.getTotalMaxMemory());
    assertEquals(this.maxNumberOfBuckets, instance.getTotalNumBuckets());
  }
  
  private PartitionAttributesImpl createPartitionAttributesImpl() {
    PartitionAttributesImpl instance = new PartitionAttributesImpl();
    instance.setColocatedWith(this.colocatedRegionFullPath);
    instance.setGlobalProperties(this.globalProps);
    instance.setLocalProperties(this.localProps);
    instance.setOffHeap(this.offHeap);
    instance.setPartitionResolver(this.partitionResolver);
    instance.setRecoveryDelay(this.recoveryDelay);
    instance.setRedundantCopies(this.redundancy);
    instance.setStartupRecoveryDelay(this.startupRecoveryDelay);
    instance.setTotalMaxMemory(this.totalMaxMemory);
    instance.setTotalNumBuckets(this.maxNumberOfBuckets);
    return instance;
  }

  private void fillInForSetAllWithPropertiesTest(PartitionAttributesImpl instance) {
    instance.setColocatedWith(this.colocatedRegionFullPath);
    
    instance.setLocalProperties(this.localProps);
    instance.setGlobalProperties(this.globalProps);
    
    this.fixedPartitionAttributes = new FixedPartitionAttributesImpl();
    this.fixedPartitionAttributes.setPartitionName("setPartitionName");
    instance.addFixedPartitionAttributes(this.fixedPartitionAttributes);
    
    instance.setLocalMaxMemory(this.localMaxMemory);
    
    this.offHeap = true;
    instance.setOffHeap(this.offHeap);
    
    //instance.addPartitionListener(this.partitionListener);
    instance.setPartitionResolver(this.partitionResolver);
    instance.setRecoveryDelay(this.recoveryDelay);
    instance.setRedundantCopies(this.redundancy);
    instance.setStartupRecoveryDelay(this.startupRecoveryDelay);
    instance.setTotalMaxMemory(this.totalMaxMemory);
    instance.setTotalNumBuckets(this.maxNumberOfBuckets);
  }
}
