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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.FixedPartitionAttributes;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.PartitionAttributesFactory;
import org.apache.geode.cache.PartitionResolver;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.configuration.DeclarableType;
import org.apache.geode.cache.configuration.RegionAttributesType;
import org.apache.geode.cache.partition.PartitionListener;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.ClassPathLoader;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.offheap.OffHeapStorage;

/**
 * Internal implementation of PartitionAttributes. New attributes existing only in this class and
 * not in {@link PartitionAttributes} are for internal use only.
 *
 * @since GemFire 5.5
 */
public class PartitionAttributesImpl implements PartitionAttributes, Cloneable, DataSerializable {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -7120239286748961954L;

  private static final int OFF_HEAP_LOCAL_MAX_MEMORY_PLACEHOLDER = 1;

  /** Partition resolver. */
  private transient PartitionResolver partitionResolver;
  private transient boolean hasPartitionResolver;

  /**
   * the number of redundant copies to keep of each datum
   */
  private int redundancy = 0;
  private transient boolean hasRedundancy;

  /**
   * maximum global size of the partitioned region, in megabytes
   */
  private long totalMaxMemory = PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT;
  private transient boolean hasTotalMaxMemory;

  /**
   * local settings GLOBAL_MAX_MEMORY_PROPERTY - deprecated, use setTotalMaxMemory
   * GLOBAL_MAX_BUCKETS_PROPERTY - deprecated, use setTotalNumBuckets
   */
  private Properties localProperties = new Properties();

  /**
   * non-local settings LOCAL_MAX_MEMORY_PROPERTY - deprecated, use setLocalMaxMemory
   */
  private Properties globalProperties = new Properties();

  /*
   * This is used to artificially set the amount of available off-heap memory when no distributed
   * system is available. This value works the same way as specifying off-heap as a GemFire
   * property, so "100m" = 100 megabytes, "100g" = 100 gigabytes, etc.
   */
  private static String testAvailableOffHeapMemory = null;

  /** the amount of local memory to use, in megabytes */
  private int localMaxMemory = PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT;
  private transient boolean hasLocalMaxMemory;
  private transient boolean localMaxMemoryExists;

  /**
   * Used to determine how to calculate the default local max memory. This was made transient since
   * we do not support p2p backwards compat changes to values stored in a region and our PR
   * implementation stores this object in the internal PRRoot internal region.
   */
  private transient boolean offHeap = false;
  private transient boolean hasOffHeap;

  /** placeholder for javadoc for this variable */
  private int totalNumBuckets = PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
  private transient boolean hasTotalNumBuckets;

  /**
   * Specifies the partition region name with which this newly created partitione region is
   * colocated
   */
  private String colocatedRegionName;
  private transient boolean hasColocatedRegionName;

  /**
   * Specifies how long existing members will wait before recoverying redundancy
   */
  private long recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
  private transient boolean hasRecoveryDelay;
  /**
   * Specifies how new members will wait before recoverying redundancy
   */
  private long startupRecoveryDelay = PartitionAttributesFactory.STARTUP_RECOVERY_DELAY_DEFAULT;
  private transient boolean hasStartupRecoveryDelay;

  private ArrayList<PartitionListener> partitionListeners;
  private transient boolean hasPartitionListeners;

  /**
   * the set of the static partitions defined for the region
   */
  private List<FixedPartitionAttributesImpl> fixedPAttrs;
  private transient boolean hasFixedPAttrs;

  public void setTotalNumBuckets(int maxNumberOfBuckets) {
    this.totalNumBuckets = maxNumberOfBuckets;
    this.globalProperties.setProperty(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY,
        String.valueOf(this.totalNumBuckets));
    this.hasTotalNumBuckets = true;
  }

  public void setTotalMaxMemory(long maximumMB) {
    this.totalMaxMemory = maximumMB;
    this.globalProperties.setProperty(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY,
        String.valueOf(maximumMB));
    this.hasTotalMaxMemory = true;
  }

  public void setLocalMaxMemory(int maximumMB) {
    this.localMaxMemory = maximumMB;
    this.localProperties.setProperty(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY,
        String.valueOf(this.localMaxMemory));
    this.hasLocalMaxMemory = true;
    this.localMaxMemoryExists = true;
  }

  public void setOffHeap(final boolean offHeap) {
    this.offHeap = offHeap;
    this.hasOffHeap = true;
    if (this.offHeap && !this.hasLocalMaxMemory) {
      this.localMaxMemory = computeOffHeapLocalMaxMemory();
    }
  }

  public void setColocatedWith(String colocatedRegionFullPath) {
    this.colocatedRegionName = colocatedRegionFullPath;
    this.hasColocatedRegionName = true;
  }

  public void setRecoveryDelay(long recoveryDelay) {
    this.recoveryDelay = recoveryDelay;
    this.hasRecoveryDelay = true;
  }

  public void setStartupRecoveryDelay(long startupRecoveryDelay) {
    this.startupRecoveryDelay = startupRecoveryDelay;
    this.hasStartupRecoveryDelay = true;
  }

  /**
   * Constructs an instance of {@code PartitionAttributes} with default settings.
   *
   * @see PartitionAttributesFactory
   */
  public PartitionAttributesImpl() {
    // do nothing
  }

  @Override
  public PartitionResolver getPartitionResolver() {
    return this.partitionResolver;
  }

  public void addPartitionListener(PartitionListener listener) {
    ArrayList<PartitionListener> listeners = this.partitionListeners;
    if (listeners == null) {
      ArrayList<PartitionListener> al = new ArrayList<PartitionListener>(1);
      al.add(listener);
      addPartitionListeners(al);
    } else {
      synchronized (listeners) {
        listeners.add(listener);
      }
    }
  }

  private void addPartitionListeners(ArrayList<PartitionListener> listeners) {
    this.partitionListeners = listeners;
    this.hasPartitionListeners = true;
  }

  @Override
  public int getRedundantCopies() {
    return this.redundancy;
  }

  @Override
  public int getTotalNumBuckets() {
    return this.totalNumBuckets;
  }

  @Override
  public long getTotalSize() {
    return this.getTotalMaxMemory();
  }

  @Override
  public long getTotalMaxMemory() {
    return this.totalMaxMemory;
  }

  public boolean getOffHeap() {
    return this.offHeap;
  }

  /**
   * Returns localMaxMemory that must not be a temporary placeholder for offHeapLocalMaxMemory if
   * off-heap. This must return the true final value of localMaxMemory which requires the
   * DistributedSystem to be created if off-heap. See bug #52003.
   *
   * @throws IllegalStateException if off-heap and the actual value is not yet known (because the
   *         DistributedSystem has not yet been created)
   * @see #getLocalMaxMemoryForValidation()
   */
  @Override
  public int getLocalMaxMemory() {
    if (this.offHeap && !this.localMaxMemoryExists) {
      int value = computeOffHeapLocalMaxMemory();
      if (this.localMaxMemoryExists) {
        // real value now exists so set it and return
        this.localMaxMemory = value;
      }
    }
    checkLocalMaxMemoryExists();
    return this.localMaxMemory;
  }

  /**
   * @throws IllegalStateException if off-heap and the actual value is not yet known (because the
   *         DistributedSystem has not yet been created)
   */
  private void checkLocalMaxMemoryExists() {
    if (this.offHeap && !this.localMaxMemoryExists) {
      // real value does NOT yet exist so throw IllegalStateException
      throw new IllegalStateException(
          "Attempting to use localMaxMemory for off-heap but value is not yet known (default value is equal to off-heap-memory-size)");
    }
  }

  /**
   * Returns localMaxMemory for validation of attributes before Region is created (possibly before
   * DistributedSystem is created). Returned value may be the temporary placeholder representing
   * offHeapLocalMaxMemory which cannot be calculated until the DistributedSystem is created. See
   * bug #52003.
   *
   * @see #OFF_HEAP_LOCAL_MAX_MEMORY_PLACEHOLDER
   * @see #getLocalMaxMemory()
   */
  public int getLocalMaxMemoryForValidation() {
    if (this.offHeap && !this.hasLocalMaxMemory && !this.localMaxMemoryExists) {
      int value = computeOffHeapLocalMaxMemory();
      if (this.localMaxMemoryExists) { // real value now exists so set it and return
        this.localMaxMemory = value;
      }
    }
    return this.localMaxMemory;
  }

  @Override
  public String getColocatedWith() {
    return this.colocatedRegionName;
  }

  @Override
  public Properties getLocalProperties() {
    return this.localProperties;
  }

  @Override
  public Properties getGlobalProperties() {
    return this.globalProperties;
  }

  @Override
  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }

  @Override
  public long getRecoveryDelay() {
    return recoveryDelay;
  }

  @Override
  public List<FixedPartitionAttributesImpl> getFixedPartitionAttributes() {
    return this.fixedPAttrs;
  }

  private static final PartitionListener[] EMPTY_PARTITION_LISTENERS = new PartitionListener[0];

  @Override
  public PartitionListener[] getPartitionListeners() {
    ArrayList<PartitionListener> listeners = this.partitionListeners;
    if (listeners == null) {
      return (PartitionListener[]) EMPTY_PARTITION_LISTENERS;
    } else {
      synchronized (listeners) {
        if (listeners.size() == 0) {
          return (PartitionListener[]) EMPTY_PARTITION_LISTENERS;
        } else {
          PartitionListener[] result = new PartitionListener[listeners.size()];
          listeners.toArray(result);
          return result;
        }
      }
    }
  }

  @Override
  public Object clone() {
    try {
      PartitionAttributesImpl copy = (PartitionAttributesImpl) super.clone();
      if (copy.fixedPAttrs != null) {
        copy.fixedPAttrs = new ArrayList<FixedPartitionAttributesImpl>(copy.fixedPAttrs);
      }
      if (copy.partitionListeners != null) {
        copy.partitionListeners = new ArrayList<PartitionListener>(copy.partitionListeners);
      }
      return copy;
    } catch (CloneNotSupportedException ignore) {
      throw new InternalGemFireError(
          "CloneNotSupportedException thrown in class that implements cloneable");
    }
  }

  public PartitionAttributesImpl copy() {
    return (PartitionAttributesImpl) clone();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    return sb.append("PartitionAttributes@").append(System.identityHashCode(this))
        .append("[redundantCopies=").append(getRedundantCopies()).append(";localMaxMemory=")
        .append(getLocalMaxMemory()).append(";totalMaxMemory=").append(this.totalMaxMemory)
        .append(";totalNumBuckets=").append(this.totalNumBuckets).append(";partitionResolver=")
        .append(this.partitionResolver).append(";colocatedWith=").append(this.colocatedRegionName)
        .append(";recoveryDelay=").append(this.recoveryDelay).append(";startupRecoveryDelay=")
        .append(this.startupRecoveryDelay).append(";FixedPartitionAttributes=")
        .append(this.fixedPAttrs).append(";partitionListeners=").append(this.partitionListeners)
        .append("]").toString();
  }

  /**
   * @throws IllegalStateException if off-heap and the actual value is not yet known (because the
   *         DistributedSystem has not yet been created)
   */
  @Override
  public void toData(DataOutput out) throws IOException {
    checkLocalMaxMemoryExists();
    out.writeInt(this.redundancy);
    out.writeLong(this.totalMaxMemory);
    out.writeInt(getLocalMaxMemory()); // call the gettor to force it to be computed in the offheap
                                       // case
    out.writeInt(this.totalNumBuckets);
    DataSerializer.writeString(this.colocatedRegionName, out);
    DataSerializer.writeObject(this.localProperties, out);
    DataSerializer.writeObject(this.globalProperties, out);
    out.writeLong(this.recoveryDelay);
    out.writeLong(this.startupRecoveryDelay);
    DataSerializer.writeObject(this.fixedPAttrs, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.redundancy = in.readInt();
    this.totalMaxMemory = in.readLong();
    this.localMaxMemory = in.readInt();
    this.totalNumBuckets = in.readInt();
    this.colocatedRegionName = DataSerializer.readString(in);
    this.localProperties = (Properties) DataSerializer.readObject(in);
    this.globalProperties = (Properties) DataSerializer.readObject(in);
    this.recoveryDelay = in.readLong();
    this.startupRecoveryDelay = in.readLong();
    this.fixedPAttrs = DataSerializer.readObject(in);
  }

  public static PartitionAttributesImpl createFromData(DataInput in)
      throws IOException, ClassNotFoundException {
    PartitionAttributesImpl result = new PartitionAttributesImpl();
    InternalDataSerializer.invokeFromData(result, in);
    return result;
  }

  public void setPartitionResolver(PartitionResolver partitionResolver) {
    this.partitionResolver = partitionResolver;
    this.hasPartitionResolver = true;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof PartitionAttributesImpl)) {
      return false;
    }

    PartitionAttributesImpl other = (PartitionAttributesImpl) obj;

    if (this.redundancy != other.getRedundantCopies()
        || getLocalMaxMemory() != other.getLocalMaxMemory() || this.offHeap != other.getOffHeap()
        || this.totalNumBuckets != other.getTotalNumBuckets()
        || this.totalMaxMemory != other.getTotalMaxMemory()
        || this.startupRecoveryDelay != other.getStartupRecoveryDelay()
        || this.recoveryDelay != other.getRecoveryDelay()
        || ((this.partitionResolver == null) != (other.getPartitionResolver() == null))
        || (this.partitionResolver != null
            && !this.partitionResolver.equals(other.getPartitionResolver()))
        || ((this.colocatedRegionName == null) != (other.getColocatedWith() == null))
        || (this.colocatedRegionName != null
            && !this.colocatedRegionName.equals(other.getColocatedWith()))
        || ((this.fixedPAttrs == null) != (other.getFixedPartitionAttributes() == null))
        || (this.fixedPAttrs != null
            && !this.fixedPAttrs.equals(other.getFixedPartitionAttributes()))) {
      return false;
    }

    PartitionListener[] otherPListeners = other.getPartitionListeners();
    PartitionListener[] thisPListeners = this.getPartitionListeners();

    if (otherPListeners.length != thisPListeners.length) {
      return false;
    }
    Set<String> otherListenerClassName = new HashSet<>();
    for (int i = 0; i < otherPListeners.length; i++) {
      PartitionListener listener = otherPListeners[i];
      otherListenerClassName.add(listener.getClass().getName());
    }
    Set<String> thisListenerClassName = new HashSet<>();
    for (int i = 0; i < thisPListeners.length; i++) {
      PartitionListener listener = thisPListeners[i];
      thisListenerClassName.add(listener.getClass().getName());
    }
    if (!thisListenerClassName.equals(otherListenerClassName)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return this.getRedundantCopies();
  }

  public int getRedundancy() {
    return redundancy;
  }

  public void setRedundantCopies(int redundancy) {
    this.redundancy = redundancy;
    this.hasRedundancy = true;
  }

  /**
   * Set local properties
   *
   * @deprecated use {@link #setLocalMaxMemory(int)} in GemFire 5.1 and later releases
   * @param localProps those properties for the local VM
   */
  @Deprecated
  public void setLocalProperties(Properties localProps) {
    this.localProperties = localProps;
    if (localProps.get(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY) != null) {
      setLocalMaxMemory(Integer
          .parseInt((String) localProps.get(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY)));
    }
  }

  /**
   * Set global properties
   *
   * @deprecated use {@link #setTotalMaxMemory(long)} and {@link #setTotalNumBuckets(int)} in
   *             GemFire 5.1 and later releases
   * @param globalProps those properties for the entire Partitioned Region
   */
  @Deprecated
  public void setGlobalProperties(Properties globalProps) {
    this.globalProperties = globalProps;
    String propVal = globalProps.getProperty(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY);
    if (propVal != null) {
      try {
        setTotalMaxMemory(Integer.parseInt(propVal));
      } catch (RuntimeException ignore) {
        this.totalMaxMemory = PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT;
      }
    }
    propVal = globalProps.getProperty(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY);
    if (propVal != null) {
      try {
        this.setTotalNumBuckets(Integer.parseInt(propVal));
      } catch (RuntimeException ignore) {
        this.totalNumBuckets = PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
      }
    }
  }

  public void addFixedPartitionAttributes(FixedPartitionAttributes fpa) {
    if (this.fixedPAttrs == null) {
      this.fixedPAttrs = new ArrayList<>(1);
      this.fixedPAttrs.add((FixedPartitionAttributesImpl) fpa);
      this.hasFixedPAttrs = true;
    } else {
      this.fixedPAttrs.add((FixedPartitionAttributesImpl) fpa);
    }
  }

  private void addFixedPartitionAttributes(List<FixedPartitionAttributesImpl> fpas) {
    this.fixedPAttrs = fpas;
    this.hasFixedPAttrs = true;
  }

  /**
   * Validates that the attributes are consistent with each other. The following rules are checked
   * and enforced:
   * <ul>
   * <li>Redundancy should be between 1 and 4</li>
   * <li>Scope should be either DIST_ACK or DIST_NO_ACK</li>
   * </ul>
   * NOTE: validation that depends on more than one attribute can not be done in this method. That
   * validation needs to be done in validateWhenAllAttributesAreSet
   *
   * @throws IllegalStateException if the attributes are not consistent with each other.
   */
  public void validateAttributes() {
    if ((this.totalNumBuckets <= 0)) {
      throw new IllegalStateException(
          String.format(
              "TotalNumBuckets %s is an illegal value, please choose a value greater than 0",
              this.totalNumBuckets));
    }
    if ((this.redundancy < 0) || (this.redundancy >= 4)) {
      throw new IllegalStateException(
          String.format(
              "RedundantCopies %s is an illegal value, please choose a value between 0 and 3",
              this.redundancy));
    }
    for (Iterator it = this.getLocalProperties().keySet().iterator(); it.hasNext();) {
      String propName = (String) it.next();
      if (!PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY.equals(propName)) {
        throw new IllegalStateException(
            String.format("Unknown local property: '%s'",
                propName));
      }
    }
    for (Iterator it = this.getGlobalProperties().keySet().iterator(); it.hasNext();) {
      String propName = (String) it.next();
      if (!PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY.equals(propName)
          && !PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY.equals(propName)) {
        throw new IllegalStateException(
            String.format("Unknown global property: '%s'",
                propName));
      }
    }
    if (this.recoveryDelay < -1) {
      throw new IllegalStateException("RecoveryDelay " + this.recoveryDelay
          + " is an illegal value, please choose a value that is greater than or equal to -1");
    }
    if (this.startupRecoveryDelay < -1) {
      throw new IllegalStateException("StartupRecoveryDelay " + this.startupRecoveryDelay
          + " is an illegal value, please choose a value that is greater than or equal to -1");
    }
    if (this.fixedPAttrs != null) {
      List<FixedPartitionAttributesImpl> duplicateFPAattrsList =
          new ArrayList<FixedPartitionAttributesImpl>();
      Set<FixedPartitionAttributes> fpAttrsSet = new HashSet<FixedPartitionAttributes>();
      for (FixedPartitionAttributesImpl fpa : this.fixedPAttrs) {
        if (fpa == null || fpa.getPartitionName() == null) {
          throw new IllegalStateException(
              "Fixed partition name cannot be null"
                  .toString());
        }
        if (fpAttrsSet.contains(fpa)) {
          duplicateFPAattrsList.add(fpa);
        } else {
          fpAttrsSet.add(fpa);
        }
      }
      if (duplicateFPAattrsList.size() != 0) {
        throw new IllegalStateException(
            String.format("Partition name %s can be added only once in FixedPartitionAttributes",
                duplicateFPAattrsList.toString()));
      }
    }
  }

  /**
   * This validation should only be done once the region attributes that owns this pa is ready to be
   * created. Need to do it this late because of bug 45749.
   */
  public void validateWhenAllAttributesAreSet(boolean isDeclarative) {
    if (this.colocatedRegionName != null) {
      if (this.fixedPAttrs != null) {
        throw new IllegalStateException(
            String.format(
                "FixedPartitionAttributes %s can not be specified in PartitionAttributesFactory if colocated-with is specified. ",
                this.fixedPAttrs));
      }
    }
    if (this.fixedPAttrs != null) {
      if (this.localMaxMemory == 0) {
        throw new IllegalStateException(
            String.format("FixedPartitionAttributes %s can not be defined for accessor",
                this.fixedPAttrs));
      }
    }
  }

  /**
   * Validates colocation of PartitionRegion <br>
   * This method used to be called when the RegionAttributes were created. But this was too early
   * since the region we are colocated with might not exist (yet). So it is now called when the PR
   * using these attributes is created. See bug 47197.
   *
   * 1. region passed in setColocatedWith should exist.<br>
   * 2. region passed should be of a PartitionedRegion <br>
   * 3. Custom partitioned should be enabled for colocated regions <br>
   * 4. totalNumBuckets should be same for colocated regions<br>
   * 5. redundancy of colocated regions should be same<br>
   *
   * @since GemFire 5.8Beta
   */
  void validateColocation(Cache cache) {
    if (this.colocatedRegionName == null) {
      return;
    }
    if (cache != null) {
      Region<?, ?> region = cache.getRegion(this.colocatedRegionName);
      if (region == null) {
        throw new IllegalStateException(
            "Region specified in 'colocated-with' is not present. It should be created before setting 'colocated-with' to this region.");
      }
      if (!(region instanceof PartitionedRegion)) {
        throw new IllegalStateException(
            "Setting the attribute 'colocated-with' is supported only for PartitionedRegions");
      }
      PartitionedRegion colocatedRegion = (PartitionedRegion) region;
      if (this.getTotalNumBuckets() != colocatedRegion.getPartitionAttributes()
          .getTotalNumBuckets()) {
        throw new IllegalStateException(
            "Current PartitionedRegion's TotalNumBuckets should be same as TotalNumBuckets of colocated PartitionedRegion");
      }
      if (this.getRedundancy() != colocatedRegion.getPartitionAttributes().getRedundantCopies()) {
        throw new IllegalStateException(
            "Current PartitionedRegion's redundancy should be same as the redundancy of colocated PartitionedRegion");
      }
    }
  }

  /**
   * Added for bug 45749. The attributes in pa are merged into this. Only attributes explicitly set
   * in pa will be merged into this. Any attribute set in pa will take precedence over an attribute
   * in this.
   *
   * @param pa the attributes to merge into this.
   * @since GemFire 7.0
   */
  public void merge(PartitionAttributesImpl pa) {
    if (pa.hasRedundancy) {
      setRedundantCopies(pa.getRedundantCopies());
    }
    if (pa.hasLocalMaxMemory) {
      setLocalMaxMemory(pa.getLocalMaxMemory());
    }
    if (pa.hasOffHeap) {
      setOffHeap(pa.getOffHeap());
    }
    if (pa.hasTotalMaxMemory) {
      setTotalMaxMemory(pa.getTotalMaxMemory());
    }
    if (pa.hasTotalNumBuckets) {
      setTotalNumBuckets(pa.getTotalNumBuckets());
    }
    if (pa.hasPartitionResolver) {
      setPartitionResolver(pa.getPartitionResolver());
    }
    if (pa.hasColocatedRegionName) {
      setColocatedWith(pa.getColocatedWith());
    }
    if (pa.hasRecoveryDelay) {
      setRecoveryDelay(pa.getRecoveryDelay());
    }
    if (pa.hasStartupRecoveryDelay) {
      setStartupRecoveryDelay(pa.getStartupRecoveryDelay());
    }
    if (pa.hasFixedPAttrs) {
      addFixedPartitionAttributes(pa.getFixedPartitionAttributes());
    }
    if (pa.hasPartitionListeners) {
      this.addPartitionListeners(pa.partitionListeners);
    }
  }

  @SuppressWarnings("unchecked")
  public void setAll(PartitionAttributes pa) {
    setRedundantCopies(pa.getRedundantCopies());
    setLocalProperties(pa.getLocalProperties());
    setGlobalProperties(pa.getGlobalProperties());
    setLocalMaxMemory(pa.getLocalMaxMemory());
    setTotalMaxMemory(pa.getTotalMaxMemory());
    setTotalNumBuckets(pa.getTotalNumBuckets());
    setPartitionResolver(pa.getPartitionResolver());
    setColocatedWith(pa.getColocatedWith());
    setRecoveryDelay(pa.getRecoveryDelay());
    setStartupRecoveryDelay(pa.getStartupRecoveryDelay());
    setOffHeap(((PartitionAttributesImpl) pa).getOffHeap());
    addFixedPartitionAttributes(pa.getFixedPartitionAttributes());
  }

  /**
   * Only used for testing. Sets the amount of available off-heap memory when no distributed system
   * is available. This method must be called before any instances of PartitionAttributesImpl are
   * created. Specify the value the same way the off-heap memory property is specified. So, "100m" =
   * 100 megabytes, etc.
   *
   * @param newTestAvailableOffHeapMemory The new test value for available off-heap memory.
   */
  public static void setTestAvailableOffHeapMemory(final String newTestAvailableOffHeapMemory) {
    testAvailableOffHeapMemory = newTestAvailableOffHeapMemory;
  }

  /**
   * By default the partition can use up to 100% of the allocated off-heap memory.
   */
  private int computeOffHeapLocalMaxMemory() {

    long availableOffHeapMemoryInMB = 0;
    if (testAvailableOffHeapMemory != null) {
      availableOffHeapMemoryInMB =
          OffHeapStorage.parseOffHeapMemorySize(testAvailableOffHeapMemory) / (1024 * 1024);
    } else if (InternalDistributedSystem.getAnyInstance() == null) {
      this.localMaxMemoryExists = false;
      // fix 52033: return non-negative, non-zero temporary placeholder for offHeapLocalMaxMemory
      return OFF_HEAP_LOCAL_MAX_MEMORY_PLACEHOLDER;
    } else {
      String offHeapSizeConfigValue =
          InternalDistributedSystem.getAnyInstance().getOriginalConfig().getOffHeapMemorySize();
      availableOffHeapMemoryInMB =
          OffHeapStorage.parseOffHeapMemorySize(offHeapSizeConfigValue) / (1024 * 1024);
    }

    if (availableOffHeapMemoryInMB > Integer.MAX_VALUE) {
      logger.warn(
          "Reduced local max memory for partition attribute when setting from available off-heap memory size");
      return Integer.MAX_VALUE;
    }

    this.localMaxMemoryExists = true;
    return (int) availableOffHeapMemoryInMB;
  }

  public int getLocalMaxMemoryDefault() {
    if (!this.offHeap) {
      return PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT;
    }

    return computeOffHeapLocalMaxMemory();
  }

  public static PartitionAttributesImpl fromConfig(
      RegionAttributesType.PartitionAttributes configAttributes) {
    PartitionAttributesImpl partitionAttributes = new PartitionAttributesImpl();
    if (configAttributes == null) {
      return null;
    }

    if (configAttributes.getRedundantCopies() != null) {
      partitionAttributes
          .setRedundantCopies(Integer.valueOf(configAttributes.getRedundantCopies()));
    }

    if (configAttributes.getTotalMaxMemory() != null) {
      partitionAttributes.setTotalMaxMemory(Integer.valueOf(configAttributes.getTotalMaxMemory()));
    }

    if (configAttributes.getTotalNumBuckets() != null) {
      partitionAttributes
          .setTotalNumBuckets(Integer.valueOf(configAttributes.getTotalNumBuckets()));
    }

    if (configAttributes.getLocalMaxMemory() != null) {
      partitionAttributes.setLocalMaxMemory(Integer.valueOf(configAttributes.getLocalMaxMemory()));
    }

    if (configAttributes.getColocatedWith() != null) {
      partitionAttributes.setColocatedWith(configAttributes.getColocatedWith());
    }

    if (configAttributes.getPartitionResolver() != null) {
      try {
        partitionAttributes.setPartitionResolver((PartitionResolver) ClassPathLoader.getLatest()
            .forName(configAttributes.getPartitionResolver().getClassName()).newInstance());
      } catch (Exception e) {
      }
    }

    if (configAttributes.getRecoveryDelay() != null) {
      partitionAttributes.setRecoveryDelay(Long.valueOf(configAttributes.getRecoveryDelay()));
    }

    if (configAttributes.getStartupRecoveryDelay() != null) {
      partitionAttributes
          .setStartupRecoveryDelay(Long.valueOf(configAttributes.getStartupRecoveryDelay()));
    }

    if (configAttributes.getPartitionListeners() != null) {
      List<DeclarableType> configListeners = configAttributes.getPartitionListeners();
      for (int i = 0; i < configListeners.size(); i++) {
        try {
          partitionAttributes.addPartitionListener((PartitionListener) ClassPathLoader.getLatest()
              .forName(configListeners.get(i).getClassName())
              .newInstance());
        } catch (Exception e) {
        }
      }
    }

    return partitionAttributes;
  }
}
