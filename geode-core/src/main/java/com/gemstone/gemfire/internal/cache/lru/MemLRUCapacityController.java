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
package com.gemstone.gemfire.internal.cache.lru;

import java.util.Properties;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.StatisticsType;
import com.gemstone.gemfire.StatisticsTypeFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.EvictionAction;
import com.gemstone.gemfire.cache.EvictionAlgorithm;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.util.ObjectSizer;
import com.gemstone.gemfire.internal.ClassPathLoader;
import com.gemstone.gemfire.internal.statistics.StatisticsTypeFactoryImpl;
import com.gemstone.gemfire.internal.cache.AbstractLRURegionMap.CDValueWrapper;
import com.gemstone.gemfire.internal.cache.CachedDeserializableFactory;
import com.gemstone.gemfire.internal.cache.Token;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;


/**
 * A <code>CapacityController</code> that will remove the least
 * recently used (LRU) entry from a region once the region reaches a
 * certain byte {@linkplain #setMaximumMegabytes capacity}. Capacity
 * is determined by monitoring the size of entries added and evicted.
 * Capacity is specified in terms of megabytes.  GemFire uses an
 * efficient algorithm to determine the amount of space a region entry
 * occupies in the VM.  However, this algorithm may not yield optimal
 * results for all kinds of data.  The user may provide his or her own
 * algorithm for determining the size of objects by implementing an
 * {@link ObjectSizer}.
 *
 * <P>MemLRUCapacityController must be set in the {@link
 * RegionAttributes} before the region is created. A Region with
 * MemLRUCapacityController set will throw an {@link
 * IllegalStateException} if an attempt is made to replace the
 * Region's capacity controller. While the capacity controller cannot
 * be replaced, it does support changing the limit with the {@link
 * #setMaximumMegabytes} method.
 *
 * <P>
 * If you are using a <code>cache.xml</code> file to create a JCache
 * region declaratively, you can include the following to associate a
 * <code>MemLRUCapacityController</code> with a region:
 *
 *  <pre>
 *  &lt;region-attributes&gt;
 *    &lt;capacity-controller&gt;
 *      &lt;classname&gt;com.gemstone.gemfire.cache.MemLRUCapacityController&lt;/classname&gt;
 *         &lt;parameter name="maximum-megabytes"&gt;
 *           &lt;string&gt;50&lt;/string&gt;
 *         &lt;/parameter&gt;
 *         &lt;parameter name="eviction-action"&gt;
 *           &lt;string&gt;overflow-to-disk&lt;/string&gt;
 *         &lt;/parameter&gt;
 *    &lt;/capacity-controller&gt;
 *  &lt;/region-attributes&gt;
 *  </pre>
 *
 * @see LRUCapacityController
 *
 *
 * @since GemFire 2.0.2
 */
public final class MemLRUCapacityController extends LRUAlgorithm
  implements Declarable {

  private static final long serialVersionUID = 6364183985590572514L;

  private static final int OVERHEAD_PER_ENTRY = 250;

  /** The default maximum number of entries allowed by MemLRU capacity
   * controller is 10 megabytes. */
  public static final int DEFAULT_MAXIMUM_MEGABYTES = EvictionAttributes.DEFAULT_MEMORY_MAXIMUM;

  /** The key for setting the maximum-entries
   * property declaratively.
   *
   * @see #init */
  public static final String MAXIMUM_MEGABYTES = "maximum-megabytes";

  /** The {@link #init initialization} property that specifies the
   * name of the {@link ObjectSizer} implementation class. */
  public static final String SIZER_IMPL = "sizer";

  private static final long ONE_MEG = 1024L * 1024L;

  protected static final StatisticsType statType;

  static {
    // create the stats type for MemLRU.
    StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    
    final String bytesAllowedDesc = 
      "Number of total bytes allowed in this region.";
    final String byteCountDesc = 
      "Number of bytes in region.";
    final String lruEvictionsDesc = 
      "Number of total entry evictions triggered by LRU.";
    final String lruDestroysDesc = "Number of entries destroyed in the region through both destroy cache operations and eviction. Reset to zero each time it exceeds lruDestroysLimit.";
    final String lruDestroysLimitDesc =
      "Maximum number of entry destroys triggered by LRU before scan occurs.";
    final String lruEvaluationsDesc = 
      "Number of entries evaluated during LRU operations.";
    final String lruGreedyReturnsDesc =
      "Number of non-LRU entries evicted during LRU operations";
     
    statType = f.createType( "MemLRUStatistics",
      "Statistics about byte based Least Recently Used region entry disposal",
      new StatisticDescriptor[] {
        f.createLongGauge("bytesAllowed", bytesAllowedDesc, "bytes" ),
        f.createLongGauge("byteCount", byteCountDesc, "bytes" ),
        f.createLongCounter("lruEvictions", lruEvictionsDesc, "entries" ),
        f.createLongCounter("lruDestroys", lruDestroysDesc, "entries" ),
        f.createLongGauge("lruDestroysLimit", lruDestroysLimitDesc, "entries" ),
        f.createLongCounter("lruEvaluations", lruEvaluationsDesc, "entries" ),
        f.createLongCounter("lruGreedyReturns", lruGreedyReturnsDesc, "entries"),
      }
    );
  }

  ////////////////////  Instance Fields  ////////////////////

  private long limit = (DEFAULT_MAXIMUM_MEGABYTES) * ONE_MEG;

  private ObjectSizer sizer;

  private int perEntryOverHead = OVERHEAD_PER_ENTRY;
  
  private final boolean isOffHeap;

  ///////////////////////  Constructors  ///////////////////////

  /**
   * Create an instance of the capacity controller with default
   * settings.  The default settings are 0
   * <code>maximum-megabytes</code> and a default <code>sizer</code>,
   * requiring either the {@link #init} method to be called, or the
   * {@link #setMaximumMegabytes} method.
   */
  public MemLRUCapacityController(Region region) {
    this(DEFAULT_MAXIMUM_MEGABYTES,region);
  }

  /**
   * Create an instance of the capacity controller the given settings.
   * 
   * @param megabytes
   *                the amount of memory allowed in this region specified in
   *                megabytes.<br>
   *                <p>
   *                For a region with
   *                {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION}, it
   *                is overridden by
   *                {@link  com.gemstone.gemfire.cache.PartitionAttributesFactory#setLocalMaxMemory(int)  " local max memory "}
   *                specified for the
   *                {@link com.gemstone.gemfire.cache.PartitionAttributes}. It
   *                signifies the amount of memory allowed in the region,
   *                collectively for its primary buckets and redundant copies
   *                for this VM. It can be different for the same region in
   *                different VMs.
   */
  public MemLRUCapacityController( int megabytes,Region region )  {
    this( megabytes, null /* sizerImpl */,region );
  }


  /**
   * Create an instance of the capacity controller the given settings.
   * 
   * @param megabytes
   *                the amount of memory allowed in this region specified in
   *                megabytes.<br>
   *                <p>
   *                For a region with
   *                {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION}, it
   *                is overridden by
   *                {@link  com.gemstone.gemfire.cache.PartitionAttributesFactory#setLocalMaxMemory(int)  " local max memory "}
   *                specified for the
   *                {@link com.gemstone.gemfire.cache.PartitionAttributes}. It
   *                signifies the amount of memory allowed in the region,
   *                collectively for its primary buckets and redundant copies
   *                for this VM. It can be different for the same region in
   *                different VMs.
   * @param sizerImpl
   *                classname of a class that implements ObjectSizer, used to
   *                compute object sizes for MemLRU
   */
  public MemLRUCapacityController( int megabytes , ObjectSizer sizerImpl,Region region)  {
    this( megabytes, sizerImpl, EvictionAction.DEFAULT_EVICTION_ACTION ,region, false);
  }

  /**
   * Create an instance of the capacity controller the given settings.
   * 
   * @param megabytes
   *                the amount of memory allowed in this region specified in
   *                megabytes.<br>
   *                <p>
   *                For a region with
   *                {@link com.gemstone.gemfire.cache.DataPolicy#PARTITION}, it
   *                is overridden by
   *                {@link  com.gemstone.gemfire.cache.PartitionAttributesFactory#setLocalMaxMemory(int)  " local max memory "}
   *                specified for the
   *                {@link com.gemstone.gemfire.cache.PartitionAttributes}. It
   *                signifies the amount of memory allowed in the region,
   *                collectively for its primary buckets and redundant copies
   *                for this VM. It can be different for the same region in
   *                different VMs.
   * @param sizerImpl
   *                classname of a class that implements ObjectSizer, used to
   *                compute object sizes for MemLRU
   * @param isOffHeap true if the region that owns this cc is stored off heap
   */
  public MemLRUCapacityController( int megabytes , ObjectSizer sizerImpl,
                                   EvictionAction evictionAction,Region region, boolean isOffHeap)  {
    super(evictionAction,region);
    this.isOffHeap = isOffHeap;
    setMaximumMegabytes(megabytes);
    setSizer(sizerImpl);
  }

  //////////////////////  Instance Methods  /////////////////////

  /**
   * Declaratively initializes this capacity controller.  Supported
   * properties are:
   *
   * <ul>
   * <li>{@link #MAXIMUM_MEGABYTES maximum-megabytes}: The number of
   *      megabytes to limit the region to.</li> 
   * <li>{@link #EVICTION_ACTION eviction-action}: The action to
   *     perform when the LRU region entry is evicted.</li>
   * <li>{@link #SIZER_IMPL sizer}: The name of the {@link
   *     ObjectSizer} implementation class to use for computing the
   *     size of region entries.</li>
   * </ul>
   *
   * @throws NumberFormatException
   *         The <code>maximum-megabytes</code> property cannot be
   *         parsed as an integer
   * @throws IllegalArgumentException
   *         The value of the <code>eviction-action</code> property is
   *         not recoginzed.
   */
  public void init(Properties props) {
    String prop = null;
    String sizerStr = null;
    if ( ( sizerStr = props.getProperty(SIZER_IMPL) ) != null ) {
      try {
        Class c = ClassPathLoader.getLatest().forName(sizerStr);
        setSizer((ObjectSizer)c.newInstance());
      }
      catch(Exception e) {
        IllegalArgumentException ex = new IllegalArgumentException(LocalizedStrings.MemLRUCapacityController_COULD_NOT_CREATE_SIZER_INSTANCE_GIVEN_THE_CLASS_NAME_0.toLocalizedString(sizer));
        ex.initCause(e);
        throw ex;
      }
    }
    
    if ( ( prop = props.getProperty( MAXIMUM_MEGABYTES ) ) != null ) {
      this.limit = Integer.parseInt( prop ) * ONE_MEG;
    }

    if ( ( prop = props.getProperty( EVICTION_ACTION ) ) != null ) {
      setEvictionAction(EvictionAction.parseAction(prop));
    }
  }

  // Candidate for removal since capacity controller no longer part of 
  // cache.xml
  @Override
  public Properties getProperties() {
    Properties props = new Properties();
    if (this.evictionAction != EvictionAction.DEFAULT_EVICTION_ACTION) {
      props.setProperty(EVICTION_ACTION, this.evictionAction.toString());
    }
    long megLimit = this.limit / ONE_MEG;
    if (megLimit != DEFAULT_MAXIMUM_MEGABYTES) {
      props.setProperty(MAXIMUM_MEGABYTES, String.valueOf(megLimit));
    }
    if (this.sizer != null) {
      props.setProperty(SIZER_IMPL, this.sizer.getClass().getName());
    }
    return props;
  }

  /**
   * Reset the maximum allowed limit on memory to use for this region. This
   * change takes effect on next region operation that could increase the
   * region's byte size. If the region is shared, this change is seen by all vms
   * on using the same GemFire shared memory system.
   */
  public void setMaximumMegabytes( int megabytes ) {
    if (megabytes <= 0) {
      throw new IllegalArgumentException(LocalizedStrings.MemLRUCapacityController_MEMLRUCONTROLLER_LIMIT_MUST_BE_POSTIVE_0
          .toLocalizedString(Integer.valueOf(megabytes)));
    }
    this.limit = (megabytes) * ONE_MEG;
    if (bucketRegion != null) {
      bucketRegion.setLimit(this.limit);
    }
    else if (this.stats != null) {
      this.stats.setLimit(this.limit);
    }
  }
  
  @Override
  public void setLimit(int maximum) {
    setMaximumMegabytes(maximum);
  }

  /**
   * Sets the the number of bytes of overhead each object occupies in
   * the VM.  This value may vary between VM implementations.
   */
  public void setEntryOverHead(int entryOverHead) {
    this.perEntryOverHead = entryOverHead;
  }

//   public void writeExternal(ObjectOutput out)
//     throws IOException {
//     super.writeExternal(out);
//     if (this.stats != null) {
//       long limit = this.getLRUHelper().limit();
//       Assert.assertTrue(limit > 0);
//       out.writeLong(limit);

//     } else {
//       Assert.assertTrue(this.limit > 0);
//       out.writeLong(this.limit);
//     }
//     if (this.sizer != null) {
//       out.writeBoolean(true);
//       out.writeUTF(this.sizer.getClass().getName());

//     } else {
//       out.writeBoolean(false);
//     }
//   }

//   public void readExternal(ObjectInput in)
//     throws IOException, ClassNotFoundException {
//     super.readExternal(in);
//     long limit = in.readLong();
//     setMaximumMegabytes((int) limit);
//     if (in.readBoolean()) {
//       String className = in.readUTF();
//       setSizer(className);
//     }
//   }

  /**
   * Sets the {@link ObjectSizer} used to calculate the size of
   * objects placed in the cache.
   *
   * @param sizer
   *        The name of the sizer class
   */
  private void setSizer(ObjectSizer sizer) {
    this.sizer = sizer;
  }

  @Override
  public long getLimit() {
    return this.limit;
  }

  @Override
  protected EnableLRU createLRUHelper() {
    return new AbstractEnableLRU() {

      /**
       * Indicate what kind of <code>EvictionAlgorithm</code> this helper implements 
       */
      public EvictionAlgorithm getEvictionAlgorithm() {
        return EvictionAlgorithm.LRU_MEMORY;
      }


      /**
       * compute the size of storing a key/value pair in the cache..
       */
      public int entrySize( Object key, Object value )
        throws IllegalArgumentException {

        if (value == Token.TOMBSTONE) {
          return 0;
        }
        
        int size = 0;
        int keySize = 0;
        if (!MemLRUCapacityController.this.isOffHeap) {
          size += MemLRUCapacityController.this.getPerEntryOverhead();
          keySize = sizeof(key);
        }
        int valueSize = sizeof(value);
//         com.gemstone.gemfire.internal.cache.GemFireCacheImpl.getInstance().getLogger().info("DEBUG MemLRUCC: overhead=" + size
//                                                     + " keySize=" + keySize
//                                                     + " valueSize=" + valueSize);
        size += keySize;
        size += valueSize;
        return size;
      }

      public StatisticsType getStatisticsType() {
        return statType;
      }

      public String getStatisticsName() {
        return "MemLRUStatistics";
      }

      public int getLimitStatId() {
        return statType.nameToId("bytesAllowed");
      }

      public int getCountStatId() {
        return statType.nameToId("byteCount");
      }

      public int getEvictionsStatId() {
        return statType.nameToId("lruEvictions");
      }

      public int getDestroysStatId() {
        return statType.nameToId("lruDestroys");
      }

      public int getDestroysLimitStatId() {
        return statType.nameToId("lruDestroysLimit");
      }

      public int getEvaluationsStatId() {
        return statType.nameToId("lruEvaluations");
      }
      
      public int getGreedyReturnsStatId() {
        return statType.nameToId("lruGreedyReturns");
      }

      public boolean mustEvict(LRUStatistics stats, Region region, int delta) {
        return stats.getCounter() + delta > stats.getLimit();
      }
    };
  }

  // added to fix bug 40718
  static int basicSizeof(Object o, ObjectSizer sizer)
      throws IllegalArgumentException {
    final boolean cdChangingForm = o instanceof CDValueWrapper;
    if (cdChangingForm) {
      o = ((CDValueWrapper)o).getValue();
    }
    if (o == null || o == Token.INVALID || o == Token.LOCAL_INVALID
        || o == Token.DESTROYED || o == Token.TOMBSTONE) {
      return 0;
    }

    int size;
//    Shouldn't we defer to the user's object sizer for these things?
    if ( o instanceof byte[]  || o instanceof String) {
      size = ObjectSizer.DEFAULT.sizeof(o);
    }
    else if (o instanceof Sizeable) {
      size = ((Sizeable)o).getSizeInBytes();
    }
    else if (sizer != null) {
      size = sizer.sizeof(o);
    }
    else {
      size = ObjectSizer.DEFAULT.sizeof(o);
    }
    if (cdChangingForm) {
      size += CachedDeserializableFactory.overhead();
    }
    return size;
  }
  /**
   * Return the size of an object as stored in GemFire... Typically
   * this is the serialized size in bytes..  This implementation is
   * slow....  Need to add Sizer interface and call it for customer
   * objects.
   */
  protected int sizeof( Object o ) throws IllegalArgumentException {
    return basicSizeof(o, this.sizer);
  }

  public int getPerEntryOverhead() {
    return perEntryOverHead;
  }
  
  @Override
  public boolean equals(Object cc) {
    if (!super.equals(cc)) return false;
    MemLRUCapacityController other = (MemLRUCapacityController)cc;
    if (this.limit != other.limit) return false;
    return true;
  }

  /*
   * (non-Javadoc)
   * @see java.lang.Object#hashCode()
   * 
   * Note that we just need to make sure that equal objects return equal
   * hashcodes; nothing really elaborate is done here.
   */
  @Override
  public int hashCode() {
    int result = super.hashCode();
    result += this.limit;
    return result;
  }
  
  /**
   * Returns a brief description of this capacity controller.
   *
   * @since GemFire 4.0
   */
  @Override
  public String toString() {
    return "MemLRUCapacityController with a capacity of " +
      this.getLimit() + " megabytes and and eviction action " + this.getEvictionAction();
  }
}

