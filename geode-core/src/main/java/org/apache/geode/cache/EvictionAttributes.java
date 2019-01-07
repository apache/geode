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

package org.apache.geode.cache;

import java.util.Optional;

import org.apache.geode.DataSerializable;
import org.apache.geode.cache.configuration.EnumActionDestroyOverflow;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.control.ResourceManager;
import org.apache.geode.cache.util.ObjectSizer;
import org.apache.geode.internal.cache.EvictionAttributesImpl;

/**
 * <p>
 * Attributes that describe how a <code>Region</code>'s size is managed through an eviction
 * controller. Eviction controllers are defined by an
 * {@link org.apache.geode.cache.EvictionAlgorithm} and a
 * {@link org.apache.geode.cache.EvictionAction}. Once a <code>Region</code> is created with an
 * eviction controller, it can not be removed, however it can be changed through an
 * {@link org.apache.geode.cache.EvictionAttributesMutator}.
 *
 * @see org.apache.geode.cache.AttributesFactory#setEvictionAttributes(EvictionAttributes)
 * @see org.apache.geode.cache.AttributesMutator
 * @since GemFire 5.0
 */
@SuppressWarnings("serial")
public abstract class EvictionAttributes implements DataSerializable {
  /**
   * The default maximum for {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU}. Currently
   * <code>900</code> entries.
   */
  public static final int DEFAULT_ENTRIES_MAXIMUM = 900;
  /**
   * The default maximum for {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU}. Currently
   * <code>10</code> megabytes.
   */
  public static final int DEFAULT_MEMORY_MAXIMUM = 10;

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with
   * default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and default
   * {@linkplain #DEFAULT_ENTRIES_MAXIMUM maximum}.
   * <p/>
   * {@link EvictionAttributes} cause regions to evict the least recently used (LRU) entry once the
   * region reaches a maximum capacity. The entry is either locally destroyed or its value overflows
   * to disk when evicted.
   * <p/>
   * <p/>
   * This is not supported when replication is enabled.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, the EvictionAttribute <code>maximum</code>,
   * indicates the number of entries allowed in the region, collectively for its primary buckets and
   * redundant copies for this JVM. Once there are <code>maximum</code> entries in the region's
   * primary buckets and redundant copies for this JVM, the least recently used entry will be
   * evicted from the bucket in which the subsequent put takes place.
   * <p/>
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can
   * include the following to configure a region for eviction
   * <p/>
   *
   * <pre>
   *         &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-entry-count maximum=&quot;900&quot; action=&quot;local-destroy&quot;/&gt;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   *
   * @return {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and default
   *         {@linkplain #DEFAULT_ENTRIES_MAXIMUM maximum}
   */
  public static EvictionAttributes createLRUEntryAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setMaximum(DEFAULT_ENTRIES_MAXIMUM);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with
   * default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and given
   * <code>maximumEntries</code>.
   *
   * @param maximumEntries the number of entries to keep in the Region
   * @return {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and
   *         <code>maximumEntries</code>
   * @see #createLRUEntryAttributes()
   */
  public static EvictionAttributes createLRUEntryAttributes(int maximumEntries) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setMaximum(maximumEntries);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with
   * default {@linkplain #DEFAULT_ENTRIES_MAXIMUM maximum} and given <code>evictionAction</code>.
   *
   * @param evictionAction the action to perform when evicting an entry
   * @return {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with default
   *         {@linkplain #DEFAULT_ENTRIES_MAXIMUM maximum} and given <code>evictionAction</code>
   * @see #createLRUEntryAttributes()
   * @since GemFire 8.1
   */
  public static EvictionAttributes createLRUEntryAttributes(EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY)
        .setAction(evictionAction).setMaximum(DEFAULT_ENTRIES_MAXIMUM);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with
   * given <code>evictionAction</code> and given <code>maximumEntries</code>.
   *
   * @param maximumEntries the number of entries to keep in the Region
   * @param evictionAction the action to perform when evicting an entry
   * @return {@linkplain EvictionAlgorithm#LRU_ENTRY entry LRU} eviction attributes with given
   *         <code>evictionAction</code> and given <code>maximumEntries</code>
   * @see #createLRUEntryAttributes()
   */
  public static EvictionAttributes createLRUEntryAttributes(int maximumEntries,
      EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_ENTRY)
        .setAction(evictionAction).setMaximum(maximumEntries);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with
   * default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and default
   * {@linkplain ObjectSizer#DEFAULT sizer}.
   * <p/>
   * Heap LRU EvictionAttributes evict the least recently used {@link Region.Entry} when heap usage
   * exceeds the {@link ResourceManager} eviction heap threshold. If the eviction heap threshold is
   * exceeded the least recently used {@link Region.Entry}s are evicted.
   * <p/>
   * <p/>
   * With other LRU-based eviction controllers, only cache actions (such as
   * {@link Region#put(Object, Object) puts} and {@link Region#get(Object) gets}) cause the LRU
   * entry to be evicted. However, with heap LRU, because the JVM's heap may be effected by more
   * than just the GemFire cache operations, a daemon thread will perform the eviction if no
   * operations are being done on the region.
   * <p/>
   * The eviction attribute's {@linkplain ObjectSizer sizer} is used to estimate how much the heap
   * will be reduced by an eviction.
   * <p/>
   * When using Heap LRU, the JVM must be launched with the <code>-Xmx</code> and <code>-Xms</code>
   * switches set to the same values. Many virtual machine implementations have additional JVM
   * switches to control the behavior of the garbage collector. We suggest that you investigate
   * tuning the garbage collector when using this type of eviction controller. A collector that
   * frequently collects is needed to keep our heap usage up to date. In particular, on the Sun
   * <A href="http://java.sun.com/docs/hotspot/gc/index.html">HotSpot</a> JVM, the
   * <code>-XX:+UseConcMarkSweepGC</code> flag needs to be set, and
   * <code>-XX:CMSInitiatingOccupancyFraction=N</code> should be set with N being a percentage that
   * is less than the {@link ResourceManager} eviction heap threshold.
   * <p/>
   * The JRockit JVM has similar flags, <code>-Xgc:gencon</code> and <code>-XXgcTrigger:N</code>,
   * which are required if using this LRU algorithm. Please Note: the JRockit gcTrigger flag is
   * based on heap free, not heap in use like the GemFire parameter. This means you need to set
   * gcTrigger to 100-N. for example, if your eviction threshold is 30 percent, you will need to set
   * gcTrigger to 70 percent.
   * <p/>
   * On the IBM JVM, the flag to get a similar collector is <code>-Xgcpolicy:gencon</code>, but
   * there is no corollary to the gcTrigger/CMSInitiatingOccupancyFraction flags, so when using this
   * feature with an IBM JVM, the heap usage statistics might lag the true memory usage of the JVM,
   * and thresholds may need to be set sufficiently high that the JVM will initiate GC before the
   * thresholds are crossed.
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can
   * include the following to create an LRU heap eviction controller:
   * <p/>
   *
   * <pre>
   *         &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-heap-percentage action=&quot;local-destroy&quot;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   * <p/>
   *
   * @return {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and default
   *         {@linkplain ObjectSizer#DEFAULT sizer}
   */
  public static EvictionAttributes createLRUHeapAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_HEAP)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setObjectSizer(ObjectSizer.DEFAULT);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with
   * default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and the given
   * <code>sizer</code>.
   *
   * @param sizer the sizer implementation used to determine the size of each entry in this region
   * @return {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action} and the given
   *         <code>sizer</code>
   * @see #createLRUHeapAttributes()
   */
  public static EvictionAttributes createLRUHeapAttributes(final ObjectSizer sizer) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_HEAP)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setObjectSizer(sizer);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with
   * the given <code>evictionAction</code> and given <code>sizer</code>.
   *
   * @param sizer the sizer implementation used to determine the size of each entry in this region
   * @param evictionAction the way in which entries should be evicted
   * @return {@linkplain EvictionAlgorithm#LRU_HEAP heap LRU} eviction attributes with the given
   *         <code>evictionAction</code> and given <code>sizer</code>
   * @see #createLRUHeapAttributes()
   */
  public static EvictionAttributes createLRUHeapAttributes(final ObjectSizer sizer,
      final EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_HEAP)
        .setAction(evictionAction).setObjectSizer(sizer);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, default
   * {@linkplain ObjectSizer#DEFAULT sizer}, and default {@linkplain #DEFAULT_MEMORY_MAXIMUM
   * maximum}.
   * <p/>
   * Creates EvictionAttributes for an eviction controller that will remove the least recently used
   * (LRU) entry from a region once the region reaches a certain byte capacity. Capacity is
   * determined by monitoring the size of entries added and evicted. Capacity is specified in terms
   * of megabytes. GemFire uses an efficient algorithm to determine the amount of space a region
   * entry occupies in the JVM. However, this algorithm may not yield optimal results for all kinds
   * of data. The user may provide their own algorithm for determining the size of objects by
   * implementing an {@link ObjectSizer}.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, the EvictionAttribute <code>maximum</code>, is
   * always equal to {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "}
   * specified for the {@link PartitionAttributes}. It signifies the amount of memory allowed in the
   * region, collectively for its primary buckets and redundant copies for this JVM. It can be
   * different for the same region in different JVMs.
   * <p/>
   * If you are using a <code>cache.xml</code> file to create a Cache Region declaratively, you can
   * include the following to create an LRU memory eviction controller:
   * <p/>
   *
   * <pre>
   *          &lt;region-attributes&gt;
   *            &lt;eviction-attributes&gt;
   *               &lt;lru-memory-size maximum=&quot;10&quot; action=&quot;local-destroy&quot;&gt;
   *                  &lt;class-name&gt;com.foo.MySizer&lt;/class-name&gt;
   *                  &lt;parameter name=&quot;name&quot;&gt;
   *                     &lt;string&gt;Super Sizer&lt;/string&gt;
   *                  &lt;/parameter&gt;
   *               &lt;/lru-memory-size&gt;
   *            &lt;/eviction-attributes&gt;
   *         &lt;/region-attributes&gt;
   * </pre>
   *
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, default
   *         {@linkplain ObjectSizer#DEFAULT sizer}, and default {@linkplain #DEFAULT_MEMORY_MAXIMUM
   *         maximum}
   */
  public static EvictionAttributes createLRUMemoryAttributes() {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setMaximum(DEFAULT_MEMORY_MAXIMUM)
        .setObjectSizer(ObjectSizer.DEFAULT);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, default
   * {@linkplain ObjectSizer#DEFAULT sizer}, and given <code>maximumMegabytes</code>.
   * <p/>
   * <p/>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the
   * EvictionAttribute <code>maximum</code>, is always set to
   * {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "} specified for
   * the {@link PartitionAttributes}.
   * <p/>
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, default
   *         {@linkplain ObjectSizer#DEFAULT sizer}, and given <code>maximumMegabytes</code>
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setMaximum(maximumMegabytes)
        .setObjectSizer(null);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, given
   * <code>sizer</code>, and given <code>maximumMegabytes</code>.
   * <p/>
   * <p>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the
   * EvictionAttribute <code>maximum</code>, is always set to
   * {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "} specified for
   * the {@link PartitionAttributes}.
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, given <code>sizer</code>,
   *         and given <code>maximumMegabytes</code>
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes,
      ObjectSizer sizer) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setMaximum(maximumMegabytes)
        .setObjectSizer(sizer);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with the given <code>evictionAction</code>, given <code>sizer</code>, and given
   * <code>maximumMegabytes</code>.
   * <p/>
   * <p>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the
   * EvictionAttribute <code>maximum</code>, is always set to
   * {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "} specified for
   * the {@link PartitionAttributes}.
   *
   * @param maximumMegabytes the maximum allowed bytes in the Region
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @param evictionAction the action to take when the maximum has been reached.
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with the given
   *         <code>evictionAction</code>, given <code>sizer</code>, and given
   *         <code>maximumMegabytes</code>
   * @see #createLRUMemoryAttributes()
   */
  public static EvictionAttributes createLRUMemoryAttributes(int maximumMegabytes,
      ObjectSizer sizer, EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(evictionAction).setMaximum(maximumMegabytes).setObjectSizer(sizer);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with default {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, given
   * <code>sizer</code>, and default {@linkplain #DEFAULT_MEMORY_MAXIMUM maximum}.
   * <p/>
   * <p>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the
   * EvictionAttribute <code>maximum</code>, is always set to
   * {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "} specified for
   * the {@link PartitionAttributes}.
   *
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with default
   *         {@linkplain EvictionAction#DEFAULT_EVICTION_ACTION action}, given <code>sizer</code>,
   *         and default {@linkplain #DEFAULT_MEMORY_MAXIMUM maximum}
   * @see #createLRUMemoryAttributes()
   * @since GemFire 6.0
   */
  public static EvictionAttributes createLRUMemoryAttributes(ObjectSizer sizer) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(EvictionAction.DEFAULT_EVICTION_ACTION).setObjectSizer(sizer)
        .setMaximum(DEFAULT_MEMORY_MAXIMUM);
  }

  /**
   * Creates and returns {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes
   * with given <code>evictionAction</code>, given <code>sizer</code>, and default
   * {@linkplain #DEFAULT_MEMORY_MAXIMUM maximum}.
   * <p/>
   * <p>
   * For a region with {@link DataPolicy#PARTITION}, even if maximumMegabytes are supplied, the
   * EvictionAttribute <code>maximum</code>, is always set to
   * {@link PartitionAttributesFactory#setLocalMaxMemory(int) " local max memory "} specified for
   * the {@link PartitionAttributes}.
   *
   * @param sizer calculates the size in bytes of the key and value for an entry.
   * @param evictionAction the action to take when the maximum has been reached.
   * @return {@linkplain EvictionAlgorithm#LRU_MEMORY memory LRU} eviction attributes with given
   *         <code>evictionAction</code>, given <code>sizer</code>, and default
   *         {@linkplain #DEFAULT_MEMORY_MAXIMUM maximum}
   * @see #createLRUMemoryAttributes()
   * @since GemFire 6.0
   */
  public static EvictionAttributes createLRUMemoryAttributes(ObjectSizer sizer,
      EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LRU_MEMORY)
        .setAction(evictionAction).setObjectSizer(sizer).setMaximum(DEFAULT_MEMORY_MAXIMUM);
  }

  /**
   * An {@link ObjectSizer} is used by the {@link EvictionAlgorithm#LRU_MEMORY} algorithm to measure
   * the size of each Entry as it is entered into a Region. A default implementation is provided,
   * see {@link #createLRUMemoryAttributes()} for more. An {@link ObjectSizer} is used by
   * {@link EvictionAlgorithm#LRU_HEAP} to estimate how much heap will be saved when evicting a
   * region entry.
   *
   * @return the sizer used by {@link EvictionAlgorithm#LRU_MEMORY} or
   *         {@link EvictionAlgorithm#LRU_HEAP}, for all other algorithms null is returned.
   */
  public abstract ObjectSizer getObjectSizer();

  /**
   * The algorithm is used to identify entries that will be evicted.
   *
   * @return a non-null EvictionAlgorithm instance reflecting the configured value or NONE when no
   *         eviction controller has been configured.
   */
  public abstract EvictionAlgorithm getAlgorithm();

  /**
   * The unit of this value is determined by the definition of the {@link EvictionAlgorithm} set by
   * one of the creation methods e.g. {@link EvictionAttributes#createLRUEntryAttributes()}.
   * <ul>
   * <li>If the algorithm is LRU_ENTRY then the unit is entries.
   * <li>If the algorithm is LRU_MEMORY then the unit is megabytes.
   * <li>If the algorithm is LRU_HEAP then the unit is undefined and this method always returns
   * zero.
   * Note, in geode 1.4 and earlier, this method would throw UnsupportedOperationException for
   * LRU_HEAP.
   * </ul>
   *
   * @return maximum value used by the {@link EvictionAlgorithm} which determines when the
   *         {@link EvictionAction} is performed.
   */
  public abstract int getMaximum();

  /** @return action that the {@link EvictionAlgorithm} takes when the maximum value is reached. */
  public abstract EvictionAction getAction();

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof EvictionAttributes)) {
      return false;
    }
    final EvictionAttributes other = (EvictionAttributes) obj;
    if (!getAlgorithm().equals(other.getAlgorithm())) {
      return false;
    }
    if (!getAction().equals(other.getAction())) {
      return false;
    }
    if (getMaximum() != other.getMaximum()) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return this.getAlgorithm().hashCode() ^ this.getMaximum();
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder(128);
    buffer.append(" algorithm=").append(this.getAlgorithm());
    if (!this.getAlgorithm().isNone()) {
      buffer.append("; action=").append(this.getAction());
      if (!getAlgorithm().isLRUHeap()) {
        buffer.append("; maximum=").append(this.getMaximum());
      }
      if (this.getObjectSizer() != null) {
        buffer.append("; sizer=").append(this.getObjectSizer());
      }
    }
    return buffer.toString();
  }

  /**
   * @return an EvictionAttributes for the LIFOCapacityController
   * @since GemFire 5.7
   * @deprecated For internal use only.
   */
  public static EvictionAttributes createLIFOEntryAttributes(int maximumEntries,
      EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LIFO_ENTRY)
        .setAction(evictionAction).setMaximum(maximumEntries);
  }

  /**
   * @return an EvictionAttributes for the MemLIFOCapacityController
   * @since GemFire 5.7
   * @deprecated For internal use only.
   */
  public static EvictionAttributes createLIFOMemoryAttributes(int maximumMegabytes,
      EvictionAction evictionAction) {
    return new EvictionAttributesImpl().setAlgorithm(EvictionAlgorithm.LIFO_MEMORY)
        .setAction(evictionAction).setMaximum(maximumMegabytes).setObjectSizer(null);
  }

  public RegionAttributesType.EvictionAttributes convertToConfigEvictionAttributes() {
    RegionAttributesType.EvictionAttributes configAttributes =
        new RegionAttributesType.EvictionAttributes();
    EnumActionDestroyOverflow action = EnumActionDestroyOverflow.fromValue(this.getAction()
        .toString());
    EvictionAlgorithm algorithm = getAlgorithm();
    Optional<String> objectSizerClass = Optional.ofNullable(getObjectSizer())
        .map(c -> c.getClass().toString());
    Integer maximum = getMaximum();

    if (algorithm.isLRUHeap()) {
      RegionAttributesType.EvictionAttributes.LruHeapPercentage heapPercentage =
          new RegionAttributesType.EvictionAttributes.LruHeapPercentage();
      heapPercentage.setAction(action);
      objectSizerClass.ifPresent(o -> heapPercentage.setClassName(o));
      configAttributes.setLruHeapPercentage(heapPercentage);
    } else if (algorithm.isLRUMemory()) {
      RegionAttributesType.EvictionAttributes.LruMemorySize memorySize =
          new RegionAttributesType.EvictionAttributes.LruMemorySize();
      memorySize.setAction(action);
      objectSizerClass.ifPresent(o -> memorySize.setClassName(o));
      memorySize.setMaximum(maximum.toString());
      configAttributes.setLruMemorySize(memorySize);
    } else {
      RegionAttributesType.EvictionAttributes.LruEntryCount entryCount =
          new RegionAttributesType.EvictionAttributes.LruEntryCount();
      entryCount.setAction(action);
      entryCount.setMaximum(maximum.toString());
      configAttributes.setLruEntryCount(entryCount);
    }

    return configAttributes;
  }

  public boolean isNoEviction() {
    return getAction() == EvictionAction.NONE;
  }

}
