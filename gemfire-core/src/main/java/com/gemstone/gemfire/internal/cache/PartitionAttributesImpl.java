/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.AttributesFactory;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.FixedPartitionResolver;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.PartitionAttributesFactory;
import com.gemstone.gemfire.cache.FixedPartitionAttributes;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.cache.PartitionResolver;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.partition.PartitionListener;


/**
 * Internal implementation of PartitionAttributes. New attributes existing   
 * only in this class and not in {@link PartitionAttributes} are for internal
 * use only.
 *  
 * @author Mitch Thomas
 * @since 5.5
 */
public class PartitionAttributesImpl implements PartitionAttributes,
      Cloneable, DataSerializable
{
  private static final long serialVersionUID = -7120239286748961954L;

  /** Partition resolver. */
  private transient PartitionResolver partitionResolver;
  private transient boolean hasPartitionResolver;

  /**
   * the number of redundant copies to keep of each datum */
  private int redundancy = 0;
  private transient boolean hasRedundancy;
  
  /** maximum global size of the partitioned region, in megabytes
   */
  private long totalMaxMemory = PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT;
  private transient boolean hasTotalMaxMemory;

  /** local settings
   *  GLOBAL_MAX_MEMORY_PROPERTY - deprecated, use setTotalMaxMemory
   *  GLOBAL_MAX_BUCKETS_PROPERTY - deprecated, use setTotalNumBuckets
   */
  private Properties localProperties = new Properties();

  /** non-local settings
   *  LOCAL_MAX_MEMORY_PROPERTY - deprecated, use setLocalMaxMemory
   */
  private Properties globalProperties = new Properties();
    
  // transient ExpirationAttributes entryTimeToLiveExpiration = ExpirationAttributes.DEFAULT;

  // transient ExpirationAttributes entryIdleTimeoutExpiration = ExpirationAttributes.DEFAULT;

  /** the amount of local memory to use, in megabytes */
  private int localMaxMemory = PartitionAttributesFactory.LOCAL_MAX_MEMORY_DEFAULT;
  private transient boolean hasLocalMaxMemory;

  /** placeholder for javadoc for this variable */
  private int totalNumBuckets = PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
  private transient boolean hasTotalNumBuckets;
    
  /**
   * Specifies the partition region name with which this newly created
   * partitione region is colocated
   */
  private String colocatedRegionName;
  private transient boolean hasColocatedRegionName;
  
  /**
   * Specifies how long existing members will wait before
   * recoverying redundancy
   */
  private long recoveryDelay = PartitionAttributesFactory.RECOVERY_DELAY_DEFAULT;
  private transient boolean hasRecoveryDelay;
   /**
   * Specifies how new members will wait before
   * recoverying redundancy
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
     * Constructs an instance of <code>PartitionAttributes</code> with default
     * settings.
     * 
     * @see PartitionAttributesFactory
     */
    public PartitionAttributesImpl() {
    }


    public PartitionResolver getPartitionResolver() {
      return this.partitionResolver;
    }

  public void addPartitionListener(PartitionListener listener) {
    ArrayList<PartitionListener> listeners = this.partitionListeners;
    if (listeners == null) {
      ArrayList<PartitionListener> al = new ArrayList<PartitionListener>(1);
      al.add(listener);
      addPartitionListeners(al);
    }
    else {
      synchronized (listeners) {
        listeners.add(listener);
      }
    }
  }
  private void addPartitionListeners(ArrayList<PartitionListener> listeners) {
    this.partitionListeners = listeners;
    this.hasPartitionListeners = true;
  }

  //    public ExpirationAttributes getEntryTimeToLive()
  //    {
  //      return new ExpirationAttributes(this.entryTimeToLiveExpiration.getTimeout(),
  //          this.entryTimeToLiveExpiration.getAction());
  //    }
  //
  //    public ExpirationAttributes getEntryIdleTimeout()
  //    {
  //      return new ExpirationAttributes(this.entryIdleTimeoutExpiration.getTimeout(),
  //          this.entryIdleTimeoutExpiration.getAction());
  //    }

  public int getRedundantCopies() {
    return this.redundancy;
  }

  public int getTotalNumBuckets() {
    return this.totalNumBuckets;
  }

  // deprecated method
  public long getTotalSize() {
    return this.getTotalMaxMemory();
  }
    
  public long getTotalMaxMemory() {
    return this.totalMaxMemory;
  }
    
  public int getLocalMaxMemory() {
    return this.localMaxMemory;
  }
    
  public String getColocatedWith() {
    return this.colocatedRegionName;
  }
  public Properties getLocalProperties() {
    return this.localProperties;
  }

  public Properties getGlobalProperties() {
    return this.globalProperties;
  }
  
  public long getStartupRecoveryDelay() {
    return startupRecoveryDelay;
  }
  
  public long getRecoveryDelay() {
    return recoveryDelay;
  }
  
  public List<FixedPartitionAttributesImpl> getFixedPartitionAttributes() {
    return this.fixedPAttrs;
  }

  private static final PartitionListener[] EMPTY_PARTITION_LISTENERS = new PartitionListener[0];

  public PartitionListener[] getPartitionListeners() {
    ArrayList<PartitionListener> listeners = this.partitionListeners;
    if (listeners == null) {
      return (PartitionListener[])EMPTY_PARTITION_LISTENERS;
    }
    else {
      synchronized (listeners) {
        if (listeners.size() == 0) {
          return (PartitionListener[])EMPTY_PARTITION_LISTENERS;
        }
        else {
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
    }
    catch (CloneNotSupportedException e) {
      throw new InternalGemFireError(
        LocalizedStrings.PartitionAttributesImpl_CLONENOTSUPPORTEDEXCEPTION_THROWN_IN_CLASS_THAT_IMPLEMENTS_CLONEABLE.toLocalizedString());
    }
  }
  
  public PartitionAttributesImpl copy() {
    return (PartitionAttributesImpl) clone();
  }

  @Override
    public String toString()
    {
      StringBuffer s = new StringBuffer();
      return s.append("PartitionAttributes@")
        .append(System.identityHashCode(this))
        .append("[redundantCopies=").append(getRedundantCopies())
        .append(";localMaxMemory=").append(this.localMaxMemory)
        .append(";totalMaxMemory=").append(this.totalMaxMemory)
        .append(";totalNumBuckets=").append(this.totalNumBuckets)
        .append(";partitionResolver=").append(this.partitionResolver)
        .append(";colocatedWith=").append(this.colocatedRegionName)
        .append(";recoveryDelay=").append(this.recoveryDelay)
        .append(";startupRecoveryDelay=").append(this.startupRecoveryDelay)
        .append(";FixedPartitionAttributes=").append(this.fixedPAttrs)
        .append(";partitionListeners=").append(this.partitionListeners)
        .append("]") .toString();
  }

  public String getStringForSQLF() {
    final StringBuilder sb = new StringBuilder();
    return sb.append("redundantCopies=").append(getRedundantCopies()).append(
        ",totalMaxMemory=").append(this.totalMaxMemory).append(
        ",totalNumBuckets=").append(this.totalNumBuckets).append(
        ",colocatedWith=").append(this.colocatedRegionName).append(
        ",recoveryDelay=").append(this.recoveryDelay).append(
        ",startupRecoveryDelay=").append(this.startupRecoveryDelay).toString();
  }

    public void toData(DataOutput out) throws IOException {
      out.writeInt(this.redundancy);
      out.writeLong(this.totalMaxMemory);
      out.writeInt(this.localMaxMemory);
      out.writeInt(this.totalNumBuckets);
      DataSerializer.writeString(this.colocatedRegionName, out);
      DataSerializer.writeObject(this.localProperties, out);
      DataSerializer.writeObject(this.globalProperties, out);
      out.writeLong(this.recoveryDelay);
      out.writeLong(this.startupRecoveryDelay);
      DataSerializer.writeObject(this.fixedPAttrs, out);
    }
    public void fromData(DataInput in) throws IOException,
        ClassNotFoundException {
      this.redundancy = in.readInt();
      this.totalMaxMemory = in.readLong();
      this.localMaxMemory = in.readInt();
      this.totalNumBuckets = in.readInt();
      this.colocatedRegionName = DataSerializer.readString(in);
      this.localProperties = (Properties)DataSerializer.readObject(in);
      this.globalProperties = (Properties)DataSerializer.readObject(in);
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
      
    if (! (obj instanceof PartitionAttributesImpl)) {
      return false;
    }
      
    PartitionAttributesImpl other = (PartitionAttributesImpl) obj;
      
      if (this.redundancy != other.getRedundantCopies()
          || this.localMaxMemory != other.getLocalMaxMemory()
          || this.totalNumBuckets != other.getTotalNumBuckets()
          || this.totalMaxMemory != other.getTotalMaxMemory()
          || this.startupRecoveryDelay != other.getStartupRecoveryDelay()
          || this.recoveryDelay != other.getRecoveryDelay()
//          || ! this.localProperties.equals(other.getLocalProperties())
//          || ! this.globalProperties.equals(other.getGlobalProperties())
          || ((this.partitionResolver == null) != (other.getPartitionResolver() == null))
          || (this.partitionResolver != null && !this.partitionResolver
            .equals(other.getPartitionResolver()))
          || ((this.colocatedRegionName == null) != (other.getColocatedWith() == null))
          || (this.colocatedRegionName != null && !this.colocatedRegionName
            .equals(other.getColocatedWith()))
          ||((this.fixedPAttrs == null) != (other.getFixedPartitionAttributes()== null))
          ||(this.fixedPAttrs != null && !this.fixedPAttrs.equals(other.getFixedPartitionAttributes()))
          ) {
        //throw new RuntimeException("this="+this.toString() + "   other=" + other.toString());
        return false;
        
    }

    PartitionListener[] otherPListeners = other.getPartitionListeners();
    PartitionListener[] thisPListeners = this.getPartitionListeners();

    if (otherPListeners.length != thisPListeners.length) {
      return false;
    }
    Set<String> otherListenerClassName = new HashSet<String>();
    for (int i = 0; i < otherPListeners.length; i++) {
      PartitionListener listener = otherPListeners[i];
      otherListenerClassName.add(listener.getClass().getName());
    }
    Set<String> thisListenerClassName = new HashSet<String>();
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
  public int hashCode()
  {
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
   * @deprecated use {@link #setLocalMaxMemory(int)} in GemFire 5.1 and later releases
   * @param localProps those properties for the local VM
   */
  @Deprecated
  public void setLocalProperties(Properties localProps) {
    this.localProperties = localProps;
    if (localProps.get(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY) != null) {
      setLocalMaxMemory(Integer.parseInt((String)localProps.get(PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY)));
    }
  }

  /**
   * Set global properties
   * @deprecated use {@link #setTotalMaxMemory(long)} and 
   *  {@link #setTotalNumBuckets(int)} in GemFire 5.1 and later releases
   * @param globalProps those properties for the entire Partitioned Region
   */
  @Deprecated
  public void setGlobalProperties(Properties globalProps) {
    this.globalProperties = globalProps;
    String propVal = globalProps.getProperty(PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY);
    if (propVal != null) {
      try {
        setTotalMaxMemory(Integer.parseInt(propVal)); 
      }
      catch (RuntimeException e) {
        this.totalMaxMemory = PartitionAttributesFactory.GLOBAL_MAX_MEMORY_DEFAULT;
      }
    }
    propVal = globalProps.getProperty(PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY);
    if (propVal != null) {
      try {
        this.setTotalNumBuckets(Integer.parseInt(propVal));
      }
      catch (RuntimeException e) {
        this.totalNumBuckets = PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_DEFAULT;
      }
    }
  }

  public void addFixedPartitionAttributes(FixedPartitionAttributes fpa) {
    if (this.fixedPAttrs == null) {
      this.fixedPAttrs = new ArrayList<FixedPartitionAttributesImpl>(1);
      this.fixedPAttrs.add((FixedPartitionAttributesImpl)fpa);
      this.hasFixedPAttrs = true;
    }
    else {
      this.fixedPAttrs.add((FixedPartitionAttributesImpl)fpa);
    }
  }

  private void addFixedPartitionAttributes(List<FixedPartitionAttributesImpl> fpas) {
    this.fixedPAttrs = fpas;
    this.hasFixedPAttrs = true;
  }
  
  /**
   * Validates that the attributes are consistent with each other. The following
   * rules are checked and enforced:
   * <ul>
   * <li>Redundancy should be between 1 and 4</li>
   * <li>Scope should be either DIST_ACK or DIST_NO_ACK</li>
   * </ul>
   * NOTE: validation that depends on more than one attribute can not be done in this method.
   * That validation needs to be done in validateWhenAllAttributesAreSet
   * 
   * @throws IllegalStateException
   *           if the attributes are not consistent with each other.
   */
  public void validateAttributes()
  {
    if ((this.totalNumBuckets <= 0)) {
      throw new IllegalStateException(
          LocalizedStrings.PartitionAttributesImpl_TOTALNUMBICKETS_0_IS_AN_ILLEGAL_VALUE_PLEASE_CHOOSE_A_VALUE_GREATER_THAN_0
              .toLocalizedString(Integer.valueOf(this.totalNumBuckets)));
    }
    if ((this.redundancy < 0) || (this.redundancy >= 4)) {
      throw new IllegalStateException(
        LocalizedStrings.PartitionAttributesImpl_REDUNDANTCOPIES_0_IS_AN_ILLEGAL_VALUE_PLEASE_CHOOSE_A_VALUE_BETWEEN_0_AND_3
          .toLocalizedString(Integer.valueOf(this.redundancy)));
    }
    for (Iterator it=this.getLocalProperties().keySet().iterator(); it.hasNext(); ) {
      String propName = (String)it.next();
      if (!PartitionAttributesFactory.LOCAL_MAX_MEMORY_PROPERTY.equals(propName)) {
        throw new IllegalStateException(
          LocalizedStrings.PartitionAttributesImpl_UNKNOWN_LOCAL_PROPERTY_0
            .toLocalizedString(propName));
      }
    }
    for (Iterator it=this.getGlobalProperties().keySet().iterator(); it.hasNext(); ) {
      String propName = (String)it.next();
      if (!PartitionAttributesFactory.GLOBAL_MAX_BUCKETS_PROPERTY.equals(propName)
          && !PartitionAttributesFactory.GLOBAL_MAX_MEMORY_PROPERTY.equals(propName)) {
        throw new IllegalStateException(
          LocalizedStrings.PartitionAttributesImpl_UNKNOWN_GLOBAL_PROPERTY_0
           .toLocalizedString(propName));
      }
    }
    if (this.recoveryDelay < -1) {
      throw new IllegalStateException(
          "RecoveryDelay "
              + this.recoveryDelay
              + " is an illegal value, please choose a value that is greater than or equal to -1");
    }
    if (this.startupRecoveryDelay < -1) {
      throw new IllegalStateException(
          "StartupRecoveryDelay "
              + this.startupRecoveryDelay
              + " is an illegal value, please choose a value that is greater than or equal to -1");
    }
    if (this.fixedPAttrs != null) {
        List<FixedPartitionAttributesImpl> duplicateFPAattrsList = new ArrayList<FixedPartitionAttributesImpl>();
        Set<FixedPartitionAttributes> fpAttrsSet = new HashSet<FixedPartitionAttributes>();
        for (FixedPartitionAttributesImpl fpa : this.fixedPAttrs) {
          if (fpa == null || fpa.getPartitionName() == null) {
            throw new IllegalStateException(
                LocalizedStrings.PartitionAttributesImpl_FIXED_PARTITION_NAME_CANNOT_BE_NULL
                    .toString());
          }
          if (fpAttrsSet.contains(fpa)) {
            duplicateFPAattrsList.add(fpa);
          }
          else {
            fpAttrsSet.add(fpa);
          }
        }
        if (duplicateFPAattrsList.size() != 0) {
          throw new IllegalStateException(
              LocalizedStrings.PartitionAttributesImpl_PARTITION_NAME_0_CAN_BE_ADDED_ONLY_ONCE_IN_FIXED_PARTITION_ATTRIBUTES
                  .toString(duplicateFPAattrsList.toString()));
        }
    }
  }
  
  /**
   * This validation should only be done once the region attributes
   * that owns this pa is ready to be created.
   * Need to do it this late because of bug 45749.
   */
  public void validateWhenAllAttributesAreSet(boolean isDeclarative) {
    if (this.colocatedRegionName != null) {
      if (this.fixedPAttrs != null) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionAttributesImpl_IF_COLOCATED_WITH_IS_SPECFIED_THEN_FIXED_PARTITION_ATTRIBUTES_CAN_NOT_BE_SPECIFIED
                .toLocalizedString(this.fixedPAttrs));
      }
    }
    if (this.fixedPAttrs != null) {
      if (this.localMaxMemory == 0) {
        throw new IllegalStateException(
            LocalizedStrings.PartitionAttributesImpl_FIXED_PARTITION_ATTRBUTES_0_CANNOT_BE_DEFINED_FOR_ACCESSOR
                .toString(this.fixedPAttrs));
      }
    }
  }

  /**
   * Validates colocation of PartitionRegion <br>
   * This method used to be called when the RegionAttributes were created.
   * But this was too early since the region we are colocated with might not exist (yet).
   * So it is now called when the PR using these attributes is created.
   * See bug 47197.
   * 
   * 1. region passed in setColocatedWith should exist.<br>
   * 2. region passed should be of a PartitionedRegion <br>
   * 3. Custom partitioned should be enabled for colocated regions <br>
   * 4. totalNumBuckets should be same for colocated regions<br>
   * 5. redundancy of colocated regions should be same<br>
   * 
   * @since 5.8Beta
   */
  void validateColocation() {
    if (this.colocatedRegionName == null) {
      return;
    }
    Cache cache = GemFireCacheImpl.getInstance();
    if (cache != null) {
      Region<?,?> region = cache.getRegion(this.colocatedRegionName);
      {
        if (region == null) {
          throw new IllegalStateException(LocalizedStrings.
              PartitionAttributesImpl_REGION_SPECIFIED_IN_COLOCATEDWITH_IS_NOT_PRESENT_IT_SHOULD_BE_CREATED_BEFORE_SETTING_COLOCATED_WITH_THIS_REGION
              .toLocalizedString());
        }
        if (!(region instanceof PartitionedRegion)) {
          throw new IllegalStateException(LocalizedStrings.
              PartitionAttributesImpl_SETTING_THE_ATTRIBUTE_COLOCATEDWITH_IS_SUPPORTED_ONLY_FOR_PARTITIONEDREGIONS
              .toLocalizedString());
        }
        PartitionedRegion colocatedRegion = (PartitionedRegion)region;
        if (this.getTotalNumBuckets() != colocatedRegion
            .getPartitionAttributes().getTotalNumBuckets()) {
          throw new IllegalStateException(LocalizedStrings.
              PartitionAttributesImpl_CURRENT_PARTITIONEDREGIONS_TOTALNUMBUCKETS_SHOULD_BE_SAME_AS_TOTALNUMBUCKETS_OF_COLOCATED_PARTITIONEDREGION
              .toLocalizedString());
        }
        if (this.getRedundancy() != colocatedRegion.getPartitionAttributes()
            .getRedundantCopies()) {
          throw new IllegalStateException(LocalizedStrings.
              PartitionAttributesImpl_CURRENT_PARTITIONEDREGIONS_REDUNDANCY_SHOULD_BE_SAME_AS_THE_REDUNDANCY_OF_COLOCATED_PARTITIONEDREGION
              .toLocalizedString());
        }
      }
    }
  }
  
  /**
   * Added for bug 45749.
   * The attributes in pa are merged into this.
   * Only attributes explicitly set in pa will be merged into this.
   * Any attribute set in pa will take precedence over an attribute in this.
   * @param pa the attributes to merge into this.
   * @since 7.0
   */
  public void merge(PartitionAttributesImpl pa) {
    if (pa.hasRedundancy) {
      setRedundantCopies(pa.getRedundantCopies());
    }
    if (pa.hasLocalMaxMemory) {
      setLocalMaxMemory(pa.getLocalMaxMemory());
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
    public void setAll(@SuppressWarnings("rawtypes") PartitionAttributes pa) {
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
      addFixedPartitionAttributes(pa.getFixedPartitionAttributes());
    }
  }
