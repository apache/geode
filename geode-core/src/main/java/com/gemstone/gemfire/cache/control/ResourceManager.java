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

package com.gemstone.gemfire.cache.control;

import java.util.Set;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.LowMemoryException;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Function;
import com.gemstone.gemfire.cache.query.QueryService;

/**
 * Provides support for managing resources used by the local
 * {@link com.gemstone.gemfire.cache.Cache}.
 * <p>
 * Re-balancing the GemFire Cache resources can be accomplished using a {@link
 * RebalanceOperation}:
 * <pre>
 * ResourceManager resourceManager = cache.getResourceManager();
 * RebalanceOperation rebalanceOp = resourceManager.createRebalanceFactory().start();
 * </pre>
 * Monitoring of heap utilization is enabled by setting the critical heap
 * percentage using {@link #setCriticalHeapPercentage(float)}.
 * 
 * @since 6.0
 */
public interface ResourceManager {
  
  /**
   * The default percent of heap memory at which the VM is considered in a 
   * critical state. Current value is <code>0.0</code>.
   * 
   * @see ResourceManager#setCriticalHeapPercentage(float)
   * @see ResourceManager#getCriticalHeapPercentage()
   */
  public static final float DEFAULT_CRITICAL_PERCENTAGE = 0.0f;

  /**
   * The default percent of heap memory at which the VM should begin evicting
   * data. Current value is <code>0.0</code>.
   * Note that if a HeapLRU is created and the eviction heap percentage has not
   * been set then it will default <code>80.0</code> unless the critical heap percentage
   * has been set in which case it will default to a value <code>5.0</code> less than
   * the critical heap percentage.
   * 
   * @see ResourceManager#setEvictionHeapPercentage(float)
   * @see ResourceManager#getEvictionHeapPercentage()
   */
  public static final float DEFAULT_EVICTION_PERCENTAGE = 0.0f;

  /**
   * Creates a factory for defining and starting {@link RebalanceOperation
   * RebalanceOperations}.
   * 
   * @return a factory for defining and starting RebalanceOperations
   */
  public RebalanceFactory createRebalanceFactory();
  
  /**
   * Returns a set of all active {@link RebalanceOperation
   * RebalanceOperations} that were started locally on this member.
   *
   * @return a set of all active RebalanceOperations started locally
   */
  public Set<RebalanceOperation> getRebalanceOperations();

  /**
   * Set the percentage of heap at or above which the cache is considered in
   * danger of becoming inoperable due to garbage collection pauses or out of
   * memory exceptions.
   *
   * <p>
   * Changing this value can cause {@link LowMemoryException} to be thrown from
   * the following {@link Cache} operations:
   * <ul>
   * <li>{@link Region#put(Object, Object)}
   * <li>{@link Region#put(Object, Object, Object)}
   * <li>{@link Region#create(Object, Object)}
   * <li>{@link Region#create(Object, Object, Object)}
   * <li>{@link Region#putAll(java.util.Map)}
   * <li>{@linkplain QueryService#createIndex(String, com.gemstone.gemfire.cache.query.IndexType, String, String) index creation}
   * <li>Execution of {@link Function}s whose {@link Function#optimizeForWrite()} returns true.
   * </ul>
   *
   * <p>
   * Only one change to this attribute or the eviction heap percentage will be
   * allowed at any given time and its effect will be fully realized before the
   * next change is allowed.
   * 
   * When using this threshold, the VM must be launched with the <code>-Xmx</code> and
   * <code>-Xms</code> switches set to the same values. Many virtual machine implementations
   * have additional VM switches to control the behavior of the garbage
   * collector. We suggest that you investigate tuning the garbage collector
   * when using this type of eviction controller.  A collector that frequently
   * collects is needed to keep our heap usage up to date. 
   * In particular, on the Sun <A href="http://java.sun.com/docs/hotspot/gc/index.html">HotSpot</a> VM, the
   * <code>-XX:+UseConcMarkSweepGC</code> flag needs to be set, and 
   * <code>-XX:CMSInitiatingOccupancyFraction=N</code> should be set with N being a percentage 
   * that is less than the {@link ResourceManager} critical and eviction heap thresholds.
   * 
   * The JRockit VM has similar flags, <code>-Xgc:gencon</code> and <code>-XXgcTrigger:N</code>, which are 
   * required if using this feature. Please Note: the JRockit gcTrigger flag is based on heap free, not
   * heap in use like the GemFire parameter. This means you need to set gcTrigger to 100-N. for example, if your
   * eviction threshold is 30 percent, you will need to set gcTrigger to 70 percent.
   * 
   * On the IBM VM, the flag to get a similar collector is <code>-Xgcpolicy:gencon</code>, but there is no 
   * corollary to the gcTrigger/CMSInitiatingOccupancyFraction flags, so when using this feature with an
   * IBM VM, the heap usage statistics might lag the true memory usage of the VM, and thresholds may need
   * to be set sufficiently high that the VM will initiate GC before the thresholds are crossed. 
   *
   * @param heapPercentage a percentage of the maximum tenured heap for the VM
   * @throws IllegalStateException if the heapPercentage value is not >= 0 or
   * <= 100 or when less than the current eviction heap percentage
   * @see #getCriticalHeapPercentage()
   * @see #getEvictionHeapPercentage()
   * @since 6.0
   */
  public void setCriticalHeapPercentage(float heapPercentage);
  
  /**
   * Get the percentage of heap at or above which the cache is considered in
   * danger of becoming inoperable.
   *
   * @return either the current or recently used percentage of the maximum
   * tenured heap
   * @see #setCriticalHeapPercentage(float)
   * @since 6.0
   */
  public float getCriticalHeapPercentage();
  
  /**
   * Set the percentage of off-heap at or above which the cache is considered in
   * danger of becoming inoperable due to out of memory exceptions.
   *
   * <p>
   * Changing this value can cause {@link LowMemoryException} to be thrown from
   * the following {@link Cache} operations:
   * <ul>
   * <li>{@link Region#put(Object, Object)}
   * <li>{@link Region#put(Object, Object, Object)}
   * <li>{@link Region#create(Object, Object)}
   * <li>{@link Region#create(Object, Object, Object)}
   * <li>{@link Region#putAll(java.util.Map)}
   * <li>{@linkplain QueryService#createIndex(String, com.gemstone.gemfire.cache.query.IndexType, String, String) index creation}
   * <li>Execution of {@link Function}s whose {@link Function#optimizeForWrite()} returns true.
   * </ul>
   *
   * <p>
   * Only one change to this attribute or the eviction off-heap percentage will be
   * allowed at any given time and its effect will be fully realized before the
   * next change is allowed.
   *
   * @param offHeapPercentage a percentage of the maximum off-heap memory available
   * @throws IllegalStateException if the ofHeapPercentage value is not >= 0 or
   * <= 100 or when less than the current eviction off-heap percentage
   * @see #getCriticalOffHeapPercentage()
   * @see #getEvictionOffHeapPercentage()
   * @since 9.0
   */
  public void setCriticalOffHeapPercentage(float offHeapPercentage);
  
  /**
   * Get the percentage of off-heap at or above which the cache is considered in
   * danger of becoming inoperable.
   *
   * @return either the current or recently used percentage of the maximum
   * off-heap memory
   * @see #setCriticalOffHeapPercentage(float)
   * @since 9.0
   */
  public float getCriticalOffHeapPercentage();

  /**
   * Set the percentage of heap at or above which the eviction should begin on
   * Regions configured for {@linkplain 
   * EvictionAttributes#createLRUHeapAttributes() HeapLRU eviction}.
   *
   * <p>
   * Changing this value may cause eviction to begin immediately.
   *
   * <p>
   * Only one change to this attribute or critical heap percentage will be
   * allowed at any given time and its effect will be fully realized before the
   * next change is allowed.
   *
   * This feature requires additional VM flags to perform properly. See {@linkplain 
   * ResourceManager#setCriticalHeapPercentage(float) setCriticalHeapPercentage() for details.}
   * 
   * @param heapPercentage a percentage of the maximum tenured heap for the VM
   * @throws IllegalStateException if the heapPercentage value is not >= 0 or 
   * <= 100 or when greater than the current critical heap percentage.
   * @see #getEvictionHeapPercentage()
   * @see #getCriticalHeapPercentage()
   * @since 6.0
   */
  public void setEvictionHeapPercentage(float heapPercentage);
  
  /**
   * Get the percentage of heap at or above which the eviction should begin on
   * Regions configured for {@linkplain 
   * EvictionAttributes#createLRUHeapAttributes() HeapLRU eviction}.
   *
   * @return either the current or recently used percentage of the maximum 
   * tenured heap
   * @see #setEvictionHeapPercentage(float)
   * @since 6.0
   */
  public float getEvictionHeapPercentage();

  /**
   * Set the percentage of off-heap at or above which the eviction should begin on
   * Regions configured for {@linkplain 
   * EvictionAttributes#createLRUHeapAttributes() HeapLRU eviction}.
   *
   * <p>
   * Changing this value may cause eviction to begin immediately.
   *
   * <p>
   * Only one change to this attribute or critical off-heap percentage will be
   * allowed at any given time and its effect will be fully realized before the
   * next change is allowed.
   * 
   * @param offHeapPercentage a percentage of the maximum off-heap memory available
   * @throws IllegalStateException if the offHeapPercentage value is not >= 0 or 
   * <= 100 or when greater than the current critical off-heap percentage.
   * @see #getEvictionOffHeapPercentage()
   * @see #getCriticalOffHeapPercentage()
   * @since 9.0
   */
  public void setEvictionOffHeapPercentage(float offHeapPercentage);
  
  /**
   * Get the percentage of off-heap at or above which the eviction should begin on
   * Regions configured for {@linkplain 
   * EvictionAttributes#createLRUHeapAttributes() HeapLRU eviction}.
   *
   * @return either the current or recently used percentage of the maximum 
   * off-heap memory
   * @see #setEvictionOffHeapPercentage(float)
   * @since 9.0
   */
  public float getEvictionOffHeapPercentage();
}
