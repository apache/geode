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

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.internal.cache.BucketRegion;
import com.gemstone.gemfire.internal.cache.PlaceHolderDiskRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.io.*;
import java.util.*;

/**
 * Eviction controllers that extend this class evict the least
 * recently used (LRU) entry in the region whose capacity they
 * controller.  In order to provide an efficient computation of the
 * LRU entry, GemFire uses special internal data structures for
 * managing the contents of a region.  As a result, there are several
 * restrictions that are placed on regions whose capacity is governed
 * by an LRU algorithm.
 *
 * <UL>
 *
 * <LI>If the capacity of a region is to be controlled by an LRU
 * algorithm, then the region must be <b>created</b> with 
 * {@link com.gemstone.gemfire.cache.EvictionAttributes}
 *
 * <LI>The eviction controller of a region governed by an LRU
 * algorithm cannot be changed.</LI>
 *
 * <LI>An LRU algorithm cannot be applied to a region after the region
 * has been created.</LI>
 *
 * </UL> 
 *
 * LRU algorithms also specify what {@linkplain 
 * com.gemstone.gemfire.cache.EvictionAction
 * action} should be performed upon the least recently used entry when
 * the capacity is reached.  Currently, there are two supported
 * actions: {@linkplain com.gemstone.gemfire.cache.EvictionAction#LOCAL_DESTROY locally destroying} the entry
 * (which is the {@linkplain com.gemstone.gemfire.cache.EvictionAction#DEFAULT_EVICTION_ACTION default}), thus
 * freeing up space in the VM, and {@linkplain com.gemstone.gemfire.cache.EvictionAction#OVERFLOW_TO_DISK
 * overflowing} the value of the entry to disk.
 *
 * <P>
 *
 * {@link com.gemstone.gemfire.cache.EvictionAttributes Eviction controllers} that use an LRU
 * algorithm maintain certain region-dependent state (such as the
 * maximum number of entries allowed in the region).  As a result, an
 * instance of <code>LRUAlgorithm</code> cannot be shared among
 * multiple regions.  Attempts to create a region with a LRU-based
 * capacity controller that has already been used to create another
 * region will result in an {@link IllegalStateException} being
 * thrown.
 *
 * @since GemFire 3.2
 */
public abstract class LRUAlgorithm
  implements CacheCallback, Serializable, Cloneable
{

  /** The key for setting the <code>eviction-action</code> property
   * of an <code>LRUAlgorithm</code> */
  public static final String EVICTION_ACTION = "eviction-action";

  ////////////////////////  Instance Fields  ///////////////////////

  /** What to do upon eviction */
  protected EvictionAction evictionAction;

  /** Used to dynamically track the changing region limit. */
  protected transient LRUStatistics stats;

  /** The helper created by this LRUAlgorithm */
  private transient EnableLRU helper;

  protected BucketRegion bucketRegion;
  /////////////////////////  Constructors  /////////////////////////

  /**
   * Creates a new <code>LRUAlgorithm</code> with the given
   * {@linkplain EvictionAction eviction action}.
   */
  protected LRUAlgorithm(EvictionAction evictionAction,Region region) {
    bucketRegion=(BucketRegion)(region instanceof BucketRegion ? region :null);
    setEvictionAction(evictionAction);
    this.helper = createLRUHelper();
  }

  ///////////////////////  Instance Methods  ///////////////////////

  /**
   * Used to hook up a bucketRegion late during disk recover.
   */
  public void setBucketRegion(Region r) {
    if (r instanceof BucketRegion) {
      this.bucketRegion = (BucketRegion)r;
      this.bucketRegion.setLimit(getLimit());
    }
  }
  /**
   * Sets the action that is performed on the least recently used
   * entry when it is evicted from the VM.
   *
   * @throws IllegalArgumentException
   *         If <code>evictionAction</code> specifies an unknown
   *         eviction action.
   *
   * @see EvictionAction
   */
  protected void setEvictionAction(EvictionAction  evictionAction) {
      this.evictionAction = evictionAction;
  }

  /**
   * Gets the action that is performed on the least recently used
   * entry when it is evicted from the VM.
   *
   * @return one of the following constants: {@link EvictionAction#LOCAL_DESTROY},
   * {@link EvictionAction#OVERFLOW_TO_DISK}
   */
  public EvictionAction getEvictionAction() {
    return this.evictionAction;
  }

  /**
   * For internal use only.  Returns a helper object used internally
   * by the GemFire cache implementation.
   */
  public final EnableLRU getLRUHelper() {
    synchronized (this) {
      // Synchronize with readObject/writeObject to avoid race
      // conditions with copy sharing.  See bug 31047.
      return this.helper;
    }
  }

  private void writeObject(java.io.ObjectOutputStream out)
    throws IOException {

    synchronized (this) {        // See bug 31047
      out.writeObject(this.evictionAction);
    }
  }

  private void readObject(java.io.ObjectInputStream in)
    throws IOException, ClassNotFoundException {

    synchronized (this) {        // See bug 31047
      this.evictionAction = (EvictionAction) in.readObject();
      this.helper = createLRUHelper();
    }
  }

//   public void writeExternal(ObjectOutput out)
//     throws IOException {
//     out.writeObject(this.evictionAction);
//   }

//   public void readExternal(ObjectInput in)
//     throws IOException, ClassNotFoundException {
//     String evictionAction = (String) in.readObject();
//     this.setEvictionAction(evictionAction);
//   }

//   protected Object readResolve() throws ObjectStreamException {
//     if (this.helper == null) {
//       this.helper = createLRUHelper();
//     }
//     return this;
//   }

  /**
   * Creates a new <code>LRUHelper</code> tailed for this LRU
   * algorithm implementation.
   */
  protected abstract EnableLRU createLRUHelper();

  /**
   * Returns the "limit" as defined by this LRU algorithm
   */
  public abstract long getLimit();

  /**
   * Set the limiting parameter used to determine when eviction is 
   * needed. 
   */
  public abstract void setLimit(int maximum);

  /**
   * This method is an artifact when eviction controllers used to 
   * called capacity controllers and were configured in the cache.xml 
   * file as <code>Declarable</code>
   * @since GemFire 4.1.1
   */
  public abstract Properties getProperties();

  /**
   * Releases resources obtained by this <code>LRUAlgorithm</code>
   */
  public void close() {
    if (this.stats != null) {
      if (bucketRegion != null) {
        this.stats.incEvictions(bucketRegion.getEvictions() * -1);
        this.stats.decrementCounter(bucketRegion.getCounter());
        bucketRegion.close();
      }
      else {
        this.stats.close();
      }
    }
  }

  /**
   * Returns a copy of this LRU-based eviction controller.  
   * This method is a artifact when capacity controllers
   * were used on a <code>Region</code>
   */
  @Override
  public Object clone() throws CloneNotSupportedException {
    synchronized (this) {
      LRUAlgorithm clone = (LRUAlgorithm) super.clone();
      clone.stats = null;
      synchronized (clone) {
        clone.helper = clone.createLRUHelper();
      }
      return clone;
    }
  }
  
  /** Return true if the specified capacity controller is
   *  compatible with this
   */
  @Override
  public boolean equals(Object cc) {
    if (cc == null) return false;
    if (!getClass().isAssignableFrom(cc.getClass())) return false;
    LRUAlgorithm other = (LRUAlgorithm)cc;
    if (!other.evictionAction.equals(this.evictionAction)) return false;
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
    return this.evictionAction.hashCode();
  }

  /**
   * Force subclasses to have a reasonable <code>toString</code>
   *
   * @since GemFire 4.0
   */
  @Override
  public abstract String toString();

  //////////////////////  Inner Classes  //////////////////////

  /**
   * A partial implementation of the <code>EnableLRU</code> interface
   * that contains code common to all <code>LRUAlgorithm</code>s. 
   */
  protected abstract class AbstractEnableLRU implements EnableLRU {

    /** The region whose capacity is controller by this eviction controller */
    private volatile transient String regionName;
    
    public long limit() {
      if ( stats == null ) {
        throw new InternalGemFireException(LocalizedStrings.LRUAlgorithm_LRU_STATS_IN_EVICTION_CONTROLLER_INSTANCE_SHOULD_NOT_BE_NULL.toLocalizedString());
      }
      if (bucketRegion != null) {
        return bucketRegion.getLimit();
      }
      return stats.getLimit();
    }

    public String getRegionName() {
      return this.regionName;
    }
    public void setRegionName(Object region) {
      String fullPathName;
      if (region instanceof Region) {
        fullPathName = ((Region)region).getFullPath();
      } else if (region instanceof PlaceHolderDiskRegion) {
        PlaceHolderDiskRegion phdr = (PlaceHolderDiskRegion)region;
        if (phdr.isBucket()) {
          fullPathName = phdr.getPrName();
        } else {
          fullPathName = phdr.getName();
        }
      } else {
        throw new IllegalStateException("expected Region or PlaceHolderDiskRegion");
      }
      if (this.regionName != null && !this.regionName.equals(fullPathName)) {
        throw new IllegalArgumentException(LocalizedStrings.LRUAlgorithm_LRU_EVICTION_CONTROLLER_0_ALREADY_CONTROLS_THE_CAPACITY_OF_1_IT_CANNOT_ALSO_CONTROL_THE_CAPACITY_OF_REGION_2.toLocalizedString(new Object[] {LRUAlgorithm.this, this.regionName, fullPathName}));
      }
      this.regionName = fullPathName; // store the name not the region since
      // region is not fully constructed yet
    }
    
    protected void setStats(LRUStatistics stats) {
      LRUAlgorithm.this.stats = stats;      
    }

    public LRUStatistics initStats(Object region, StatisticsFactory sf) {
      setRegionName(region);
      final LRUStatistics stats = new LRUStatistics(sf, getRegionName(), this);
      stats.setLimit( LRUAlgorithm.this.getLimit() );
      stats.setDestroysLimit( 1000 );
      setStats(stats);
      return stats;
    }

    public LRUStatistics getStats() {
      return LRUAlgorithm.this.stats;
    }

    public EvictionAction getEvictionAction() {
      return LRUAlgorithm.this.evictionAction;
    }

    public void afterEviction() {
      // Do nothing
    }

  }

}
