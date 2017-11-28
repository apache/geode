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
package org.apache.geode.internal.cache.eviction;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.cache.EvictionAction;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.BucketRegion;
import org.apache.geode.internal.cache.PlaceHolderDiskRegion;
import org.apache.geode.internal.i18n.LocalizedStrings;

/**
 * Eviction controllers that extend this class evict the least recently used (LRU) entry in the
 * region whose capacity they controller. In order to provide an efficient computation of the LRU
 * entry, GemFire uses special internal data structures for managing the contents of a region. As a
 * result, there are several restrictions that are placed on regions whose capacity is governed by
 * an LRU algorithm.
 *
 * <ul>
 * <li>If the capacity of a region is to be controlled by an LRU algorithm, then the region must be
 * <b>created</b> with {@link org.apache.geode.cache.EvictionAttributes}
 * <li>The eviction controller of a region governed by an LRU algorithm cannot be changed.</li>
 * <li>An LRU algorithm cannot be applied to a region after the region has been created.</li>
 * </ul>
 *
 * <p>
 * LRU algorithms also specify what {@linkplain org.apache.geode.cache.EvictionAction action} should
 * be performed upon the least recently used entry when the capacity is reached. Currently, there
 * are two supported actions: {@linkplain org.apache.geode.cache.EvictionAction#LOCAL_DESTROY
 * locally destroying} the entry (which is the
 * {@linkplain org.apache.geode.cache.EvictionAction#DEFAULT_EVICTION_ACTION default}), thus freeing
 * up space in the VM, and {@linkplain org.apache.geode.cache.EvictionAction#OVERFLOW_TO_DISK
 * overflowing} the value of the entry to disk.
 *
 * <p>
 * {@link org.apache.geode.cache.EvictionAttributes Eviction controllers} that use an LRU algorithm
 * maintain certain region-dependent state (such as the maximum number of entries allowed in the
 * region). As a result, an instance of {@code AbstractEvictionController} cannot be shared among
 * multiple regions. Attempts to create a region with a LRU-based capacity controller that has
 * already been used to create another region will result in an {@link IllegalStateException} being
 * thrown.
 *
 * @since GemFire 3.2
 */
abstract class AbstractEvictionController implements EvictionController {

  /**
   * The key for setting the {@code eviction-action} property of an
   * {@code AbstractEvictionController}
   */
  protected static final String EVICTION_ACTION = "eviction-action";

  private static final int DESTROYS_LIMIT = 1000;

  /**
   * What to do upon eviction
   */
  protected EvictionAction evictionAction;

  /**
   * Used to dynamically track the changing region limit.
   */
  protected transient InternalEvictionStatistics stats;

  protected BucketRegion bucketRegion;

  /** The region whose capacity is controller by this eviction controller */
  private transient volatile String regionName;

  /**
   * Creates a new {@code AbstractEvictionController} with the given {@linkplain EvictionAction
   * eviction action}.
   */
  protected AbstractEvictionController(EvictionAction evictionAction, Region region) {
    bucketRegion = (BucketRegion) (region instanceof BucketRegion ? region : null);
    setEvictionAction(evictionAction);
  }

  /**
   * Force subclasses to have a reasonable {@code toString}
   *
   * @since GemFire 4.0
   */
  @Override
  public abstract String toString();

  /**
   * Used to hook up a bucketRegion late during disk recovery.
   */
  @Override
  public void setBucketRegion(Region region) {
    if (region instanceof BucketRegion) {
      this.bucketRegion = (BucketRegion) region;
      this.bucketRegion.setLimit(getLimit());
    }
  }

  /**
   * Gets the action that is performed on the least recently used entry when it is evicted from the
   * VM.
   *
   * @return one of the following constants: {@link EvictionAction#LOCAL_DESTROY},
   *         {@link EvictionAction#OVERFLOW_TO_DISK}
   */
  @Override
  public EvictionAction getEvictionAction() {
    return this.evictionAction;
  }

  @Override
  public synchronized EvictionStatistics getStatistics() {
    // Synchronize with readObject/writeObject to avoid race
    // conditions with copy sharing. See bug 31047.
    return stats;
  }

  /**
   * Return true if the specified capacity controller is compatible with this
   */
  @Override
  public boolean equals(Object cc) {
    if (cc == null) {
      return false;
    }
    if (!getClass().isAssignableFrom(cc.getClass())) {
      return false;
    }
    AbstractEvictionController other = (AbstractEvictionController) cc;
    if (!other.evictionAction.equals(this.evictionAction)) {
      return false;
    }
    return true;
  }

  /**
   * Note that we just need to make sure that equal objects return equal hashcodes; nothing really
   * elaborate is done here.
   */
  @Override
  public int hashCode() {
    return this.evictionAction.hashCode();
  }

  @Override
  public long limit() {
    if (stats == null) {
      throw new InternalGemFireException(
          LocalizedStrings.LRUAlgorithm_LRU_STATS_IN_EVICTION_CONTROLLER_INSTANCE_SHOULD_NOT_BE_NULL
              .toLocalizedString());
    }
    if (bucketRegion != null) {
      return bucketRegion.getLimit();
    }
    return stats.getLimit();
  }

  @Override
  public EvictionStatistics initStats(Object region, StatisticsFactory statsFactory) {
    setRegionName(region);
    InternalEvictionStatistics stats =
        new EvictionStatisticsImpl(statsFactory, getRegionName(), this);
    stats.setLimit(AbstractEvictionController.this.getLimit());
    stats.setDestroysLimit(DESTROYS_LIMIT);
    setStatistics(stats);
    return stats;
  }

  /**
   * Sets the action that is performed on the least recently used entry when it is evicted from the
   * VM.
   *
   * @throws IllegalArgumentException If {@code evictionAction} specifies an unknown eviction
   *         action.
   * @see EvictionAction
   */
  protected void setEvictionAction(EvictionAction evictionAction) {
    this.evictionAction = evictionAction;
  }

  protected String getRegionName() {
    return this.regionName;
  }

  protected void setRegionName(Object region) {
    String fullPathName;
    if (region instanceof Region) {
      fullPathName = ((Region) region).getFullPath();
    } else if (region instanceof PlaceHolderDiskRegion) {
      PlaceHolderDiskRegion placeHolderDiskRegion = (PlaceHolderDiskRegion) region;
      if (placeHolderDiskRegion.isBucket()) {
        fullPathName = placeHolderDiskRegion.getPrName();
      } else {
        fullPathName = placeHolderDiskRegion.getName();
      }
    } else {
      throw new IllegalStateException("expected Region or PlaceHolderDiskRegion");
    }

    if (this.regionName != null && !this.regionName.equals(fullPathName)) {
      throw new IllegalArgumentException(
          LocalizedStrings.LRUAlgorithm_LRU_EVICTION_CONTROLLER_0_ALREADY_CONTROLS_THE_CAPACITY_OF_1_IT_CANNOT_ALSO_CONTROL_THE_CAPACITY_OF_REGION_2
              .toLocalizedString(AbstractEvictionController.this, this.regionName, fullPathName));
    }
    this.regionName = fullPathName; // store the name not the region since
    // region is not fully constructed yet
  }

  protected void setStatistics(InternalEvictionStatistics stats) {
    this.stats = stats;
  }

  private void writeObject(ObjectOutputStream out) throws IOException {
    synchronized (this) { // See bug 31047
      out.writeObject(this.evictionAction);
    }
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    synchronized (this) { // See bug 31047
      this.evictionAction = (EvictionAction) in.readObject();
    }
  }
}
