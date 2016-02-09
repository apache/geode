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
package com.gemstone.gemfire.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.CacheCallback;
import com.gemstone.gemfire.cache.CacheListener;
import com.gemstone.gemfire.cache.CacheLoader;
import com.gemstone.gemfire.cache.CacheLoaderException;
import com.gemstone.gemfire.cache.CacheWriter;
import com.gemstone.gemfire.cache.CacheWriterException;
import com.gemstone.gemfire.cache.CustomEvictionAttributes;
import com.gemstone.gemfire.cache.CustomExpiry;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.DiskWriteAttributes;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.cache.EvictionAttributes;
import com.gemstone.gemfire.cache.ExpirationAttributes;
import com.gemstone.gemfire.cache.LoaderHelper;
import com.gemstone.gemfire.cache.MembershipAttributes;
import com.gemstone.gemfire.cache.MirrorType;
import com.gemstone.gemfire.cache.PartitionAttributes;
import com.gemstone.gemfire.cache.Region.Entry;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.RegionEvent;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.cache.SubscriptionAttributes;
import com.gemstone.gemfire.compression.CompressionException;
import com.gemstone.gemfire.compression.Compressor;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.cache.EvictionAttributesImpl;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * Provides an implementation of RegionAttributes that can be used from a VM
 * remote from the vm that created the cache.
 */
public class RemoteRegionAttributes implements RegionAttributes,
    DataSerializable {
  
  private static final long serialVersionUID = -4989613295006261809L;
  private String cacheLoaderDesc;
  private String cacheWriterDesc;
  private String capacityControllerDesc;
  private String[] cacheListenerDescs;
  private Class keyConstraint;
  private Class valueConstraint;
  private ExpirationAttributes rTtl;
  private ExpirationAttributes rIdleTimeout;
  private ExpirationAttributes eTtl;
  private String customEttlDesc;
  private ExpirationAttributes eIdleTimeout;
  private String customEIdleDesc;
  private DataPolicy dataPolicy;
  private Scope scope;
  private boolean statsEnabled;
  private boolean ignoreJTA;
  private boolean isLockGrantor;
  private int concurrencyLevel;
  private boolean concurrencyChecksEnabled;
  private float loadFactor;
  private int initialCapacity;
  private boolean earlyAck;
  private boolean multicastEnabled;
  private boolean enableGateway;
  private String gatewayHubId;
  private boolean enableSubscriptionConflation;
  private boolean publisher;
  private boolean enableAsyncConflation;
  private DiskWriteAttributes diskWriteAttributes;
  private File[] diskDirs;
  private int[] diskSizes;
  private boolean indexMaintenanceSynchronous;
  private PartitionAttributes partitionAttributes;
  private MembershipAttributes membershipAttributes;
  private SubscriptionAttributes subscriptionAttributes;
  private EvictionAttributesImpl evictionAttributes = new EvictionAttributesImpl();
  private boolean cloningEnable;
  private String poolName;
  private String diskStoreName;
  private boolean isDiskSynchronous;
  private String[] gatewaySendersDescs;
  private boolean isGatewaySenderEnabled = false;
  private String[] asyncEventQueueDescs;
  private String hdfsStoreName;
  private boolean hdfsWriteOnly;
  private String compressorDesc;
  private boolean offHeap;

  /**
   * constructs a new default RemoteRegionAttributes.
   */
  public RemoteRegionAttributes(RegionAttributes attr) {
    this.cacheLoaderDesc = getDesc(attr.getCacheLoader());
    this.cacheWriterDesc = getDesc(attr.getCacheWriter());
    this.cacheListenerDescs = getDescs(attr.getCacheListeners());
    this.keyConstraint = attr.getKeyConstraint();
    this.valueConstraint = attr.getValueConstraint();
    this.rTtl = attr.getRegionTimeToLive();
    this.rIdleTimeout = attr.getRegionIdleTimeout();
    this.eTtl = attr.getEntryTimeToLive();
    this.customEttlDesc = getDesc(attr.getCustomEntryTimeToLive());
    this.eIdleTimeout = attr.getEntryIdleTimeout();
    this.customEIdleDesc = getDesc(attr.getCustomEntryIdleTimeout());
    this.dataPolicy = attr.getDataPolicy();
    this.scope = attr.getScope();
    this.statsEnabled = attr.getStatisticsEnabled();
    this.ignoreJTA = attr.getIgnoreJTA();
    this.concurrencyLevel = attr.getConcurrencyLevel();
    this.concurrencyChecksEnabled = attr.getConcurrencyChecksEnabled();
    this.loadFactor = attr.getLoadFactor();
    this.initialCapacity = attr.getInitialCapacity();
    this.earlyAck = attr.getEarlyAck();
    this.multicastEnabled = attr.getMulticastEnabled();
//    this.enableGateway = attr.getEnableGateway();
//    this.gatewayHubId = attr.getGatewayHubId();
    this.enableSubscriptionConflation = attr.getEnableSubscriptionConflation();
    this.publisher = attr.getPublisher();
    this.enableAsyncConflation = attr.getEnableAsyncConflation();
    this.diskStoreName = attr.getDiskStoreName();
    if (this.diskStoreName == null) {
      this.diskWriteAttributes = attr.getDiskWriteAttributes();
      this.diskDirs = attr.getDiskDirs();
      this.diskSizes = attr.getDiskDirSizes();
    } else {
      this.diskWriteAttributes = null;
      this.diskDirs = null;
      this.diskSizes = null;
    }
    this.partitionAttributes = attr.getPartitionAttributes();
    this.membershipAttributes = attr.getMembershipAttributes();
    this.subscriptionAttributes = attr.getSubscriptionAttributes();
    this.cloningEnable = attr.getCloningEnabled();
    this.poolName = attr.getPoolName();
    this.isDiskSynchronous = attr.isDiskSynchronous();
    this.gatewaySendersDescs = getDescs(attr.getGatewaySenderIds().toArray());
    this.asyncEventQueueDescs = getDescs(attr.getAsyncEventQueueIds().toArray());
  	this.hdfsStoreName = attr.getHDFSStoreName();
    this.hdfsWriteOnly = attr.getHDFSWriteOnly();
    this.compressorDesc = getDesc(attr.getCompressor());
    this.offHeap = attr.getOffHeap();
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteRegionAttributes() {
  }

  public CacheLoader getCacheLoader() {
    return cacheLoaderDesc.equals("") ? null : new RemoteCacheLoader(
        cacheLoaderDesc);
  }

  public CacheWriter getCacheWriter() {
    return cacheWriterDesc.equals("") ? null : new RemoteCacheWriter(
        cacheWriterDesc);
  }

  public Class getKeyConstraint() {
    return keyConstraint;
  }

  public Class getValueConstraint() {
    return valueConstraint;
  }

  public ExpirationAttributes getRegionTimeToLive() {
    return rTtl;
  }

  public ExpirationAttributes getRegionIdleTimeout() {
    return rIdleTimeout;
  }

  public ExpirationAttributes getEntryTimeToLive() {
    return eTtl;
  }

  public CustomExpiry getCustomEntryTimeToLive() {
    return customEttlDesc.equals("") ? null : new RemoteCustomExpiry(
        customEttlDesc);
  }
  
  public ExpirationAttributes getEntryIdleTimeout() {
    return eIdleTimeout;
  }
  
  public CustomExpiry getCustomEntryIdleTimeout() {
    return customEIdleDesc.equals("") ? null : new RemoteCustomExpiry(
        customEIdleDesc);
  }

  
  public String getPoolName() {
    return poolName;
  }
  
  public Scope getScope() {
    return scope;
  }

  public CacheListener getCacheListener() {
    CacheListener[] listeners = getCacheListeners();
    if (listeners.length == 0) {
      return null;
    } else if (listeners.length == 1) {
      return listeners[0];
    } else {
      throw new IllegalStateException(LocalizedStrings.RemoteRegionAttributes_MORE_THAN_ONE_CACHE_LISTENER_EXISTS.toLocalizedString());
    }
  }
  private static final CacheListener[] EMPTY_LISTENERS = new CacheListener[0];
  public CacheListener[] getCacheListeners() {
    if (this.cacheListenerDescs == null || this.cacheListenerDescs.length == 0) {
      return EMPTY_LISTENERS;
    } else {
      CacheListener[] result = new CacheListener[this.cacheListenerDescs.length];
      for (int i=0; i < result.length; i++) {
        result[i] = new RemoteCacheListener(this.cacheListenerDescs[i]);
      }
      return result;
    }
  }


  public int getInitialCapacity() {
    return initialCapacity;
  }

  public float getLoadFactor() {
    return loadFactor;
  }

  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }
  
  public boolean getConcurrencyChecksEnabled() {
    return this.concurrencyChecksEnabled;
  }

  public boolean getStatisticsEnabled() {
    return statsEnabled;
  }

  public boolean getIgnoreJTA() {
    return ignoreJTA;
  }

  public boolean isLockGrantor() {
    return this.isLockGrantor;
  }

  public boolean getPersistBackup() {
    return getDataPolicy().withPersistence();
  }

  public boolean getEarlyAck() {
    return this.earlyAck;
  }

  public boolean getMulticastEnabled() {
    return this.multicastEnabled;
  }

  public boolean getEnableGateway() {
    return this.enableGateway;
  }

  public boolean getEnableWAN() { // deprecated in 5.0
    return this.enableGateway;
  }

  public String getGatewayHubId() {
    return this.gatewayHubId;
  }

  /*
   * @deprecated as of prPersistSprint1
   */
  @Deprecated
  public boolean getPublisher() {
    return this.publisher;
  }

  public boolean getEnableConflation() { // deprecated in 5.0
    return getEnableSubscriptionConflation();
  }

  public boolean getEnableBridgeConflation() { // deprecated in 5.7
    return getEnableSubscriptionConflation();
  }

  public boolean getEnableSubscriptionConflation() {
    return this.enableSubscriptionConflation;
  }

  public boolean getEnableAsyncConflation() {
    return this.enableAsyncConflation;
  }

  public DiskWriteAttributes getDiskWriteAttributes() {
    return this.diskWriteAttributes;
  }

  public File[] getDiskDirs() {
    return this.diskDirs;
  }

  public int[] getDiskDirSizes() {
    return this.diskSizes;
   }
  
  public MirrorType getMirrorType() {
    //checkReadiness();
    if (this.dataPolicy.isNormal() || this.dataPolicy.isPreloaded()
        || this.dataPolicy.isEmpty() || this.dataPolicy.withPartitioning()) {
      return MirrorType.NONE;
    } else if (this.dataPolicy.withReplication()) {
      return MirrorType.KEYS_VALUES;
    } else {
      throw new IllegalStateException(LocalizedStrings.RemoteRegionAttributes_NO_MIRROR_TYPE_CORRESPONDS_TO_DATA_POLICY_0.toLocalizedString(this.dataPolicy));
    }
  }
  public DataPolicy getDataPolicy() {
    //checkReadiness();
    return this.dataPolicy;
  }

  public PartitionAttributes getPartitionAttributes() {
    return this.partitionAttributes;
  }

  public MembershipAttributes getMembershipAttributes() {
    return this.membershipAttributes;
  }

  public SubscriptionAttributes getSubscriptionAttributes() {
    return this.subscriptionAttributes;
  }

  public Compressor getCompressor() {
    return compressorDesc.equals("") ? null : new RemoteCompressor(
        compressorDesc);
  }
  
  public boolean getOffHeap() {
    return this.offHeap;
  }
  
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.cacheLoaderDesc, out);
    DataSerializer.writeString(this.cacheWriterDesc, out);
    DataSerializer.writeStringArray(this.cacheListenerDescs, out);
    DataSerializer.writeString(this.capacityControllerDesc, out);
    DataSerializer.writeObject(this.keyConstraint, out);
    DataSerializer.writeObject(this.valueConstraint, out);
    DataSerializer.writeObject(this.rTtl, out);
    DataSerializer.writeObject(this.rIdleTimeout, out);
    DataSerializer.writeObject(this.eTtl, out);
    DataSerializer.writeString(this.customEttlDesc, out);
    DataSerializer.writeObject(this.eIdleTimeout, out);
    DataSerializer.writeString(this.customEIdleDesc, out);
    DataSerializer.writeObject(this.dataPolicy, out);
    DataSerializer.writeObject(this.scope, out);
    out.writeBoolean(this.statsEnabled);
    out.writeBoolean(this.ignoreJTA);
    out.writeInt(this.concurrencyLevel);
    out.writeFloat(this.loadFactor);
    out.writeInt(this.initialCapacity);
    out.writeBoolean(this.earlyAck);
    out.writeBoolean(this.multicastEnabled);
    out.writeBoolean(this.enableSubscriptionConflation);
    out.writeBoolean(this.publisher);
    out.writeBoolean(this.enableAsyncConflation);

    DataSerializer.writeObject(this.diskWriteAttributes, out);
    DataSerializer.writeObject(this.diskDirs, out);
    DataSerializer.writeObject(this.diskSizes, out);
    out.writeBoolean(this.indexMaintenanceSynchronous);
    DataSerializer.writeObject(this.partitionAttributes, out);
    DataSerializer.writeObject(this.membershipAttributes, out);
    DataSerializer.writeObject(this.subscriptionAttributes, out);
    DataSerializer.writeObject(this.evictionAttributes, out);
    out.writeBoolean(this.cloningEnable);
    DataSerializer.writeString(this.diskStoreName, out);
    out.writeBoolean(this.isDiskSynchronous);
    DataSerializer.writeStringArray(this.gatewaySendersDescs, out);
    out.writeBoolean(this.isGatewaySenderEnabled);
    
    out.writeBoolean(this.concurrencyChecksEnabled);
  
    DataSerializer.writeString(this.compressorDesc, out);
    out.writeBoolean(this.offHeap);
    DataSerializer.writeString(this.hdfsStoreName, out);
  }
  
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.cacheLoaderDesc = DataSerializer.readString(in);
    this.cacheWriterDesc = DataSerializer.readString(in);
    this.cacheListenerDescs = DataSerializer.readStringArray(in);
    this.capacityControllerDesc = DataSerializer.readString(in);
    this.keyConstraint = (Class) DataSerializer.readObject(in);
    this.valueConstraint = (Class) DataSerializer.readObject(in);
    this.rTtl = (ExpirationAttributes) DataSerializer.readObject(in);
    this.rIdleTimeout = (ExpirationAttributes) DataSerializer.readObject(in);
    this.eTtl = (ExpirationAttributes) DataSerializer.readObject(in);
    this.customEttlDesc = DataSerializer.readString(in);
    this.eIdleTimeout = (ExpirationAttributes) DataSerializer.readObject(in);
    this.customEIdleDesc = DataSerializer.readString(in);
    this.dataPolicy = (DataPolicy) DataSerializer.readObject(in);
    this.scope = (Scope) DataSerializer.readObject(in);
    this.statsEnabled = in.readBoolean();
    this.ignoreJTA = in.readBoolean();
    this.concurrencyLevel = in.readInt();
    this.loadFactor = in.readFloat();
    this.initialCapacity = in.readInt();
    this.earlyAck = in.readBoolean();
    this.multicastEnabled = in.readBoolean();
    this.enableSubscriptionConflation = in.readBoolean();
    this.publisher = in.readBoolean();
    this.enableAsyncConflation = in.readBoolean();

    this.diskWriteAttributes = (DiskWriteAttributes) DataSerializer.readObject(in);
    this.diskDirs = (File[]) DataSerializer.readObject(in);
    this.diskSizes = (int[] )DataSerializer.readObject(in);
    this.indexMaintenanceSynchronous = in.readBoolean();
    this.partitionAttributes = (PartitionAttributes) DataSerializer
    .readObject(in);
    this.membershipAttributes = (MembershipAttributes) DataSerializer
        .readObject(in);
    this.subscriptionAttributes = (SubscriptionAttributes) DataSerializer
        .readObject(in);
    this.evictionAttributes = (EvictionAttributesImpl) DataSerializer.readObject(in);
    this.cloningEnable = in.readBoolean();
    this.diskStoreName = DataSerializer.readString(in);
    this.isDiskSynchronous = in.readBoolean();
    this.gatewaySendersDescs = DataSerializer.readStringArray(in);
    this.isGatewaySenderEnabled = in.readBoolean();
    this.concurrencyChecksEnabled = in.readBoolean();
  
    this.compressorDesc = DataSerializer.readString(in);
    this.offHeap = in.readBoolean();
    this.hdfsStoreName = DataSerializer.readString(in);
  }
  
  private String[] getDescs(Object[] l) {
    if (l == null) {
      return new String[0];
    } else {
      String[] result = new String[l.length];
      for (int i=0; i < l.length; i++) {
        result[i] = getDesc(l[i]);
      }
      return result;
    }
  }
  private String getDesc(Object o) {
    if (o == null) {
      return "";
    }
    else if (o instanceof RemoteCacheCallback) {
      return ((RemoteCacheCallback) o).toString();
    }
    else {
      return o.getClass().getName();
    }
  }

  public boolean getIndexMaintenanceSynchronous() {
    return this.indexMaintenanceSynchronous;
  }

  /**
   * A remote representation of a cache callback
   */
  private abstract static class RemoteCacheCallback implements CacheCallback {

    /** The description of this callback */
    private final String desc;

    /**
     * Creates a new <code>RemoteCacheCallback</code> with the given
     * description.
     */
    protected RemoteCacheCallback(String desc) {
      this.desc = desc;
    }

    @Override
    public final String toString() {
      return desc;
    }

    public final void close() {
    }
  }

  private static class RemoteCacheListener extends RemoteCacheCallback
      implements CacheListener {

    public RemoteCacheListener(String desc) {
      super(desc);
    }

    public void afterCreate(EntryEvent event) {
    }

    public void afterUpdate(EntryEvent event) {
    }

    public void afterInvalidate(EntryEvent event) {
    }

    public void afterDestroy(EntryEvent event) {
    }

    public void afterRegionInvalidate(RegionEvent event) {
    }

    public void afterRegionDestroy(RegionEvent event) {
    }

    public void afterRegionClear(RegionEvent event) {
    }

    public void afterRegionCreate(RegionEvent event) {
    }
    
    public void afterRegionLive(RegionEvent event) {
    }    
  }

  private static class RemoteCacheWriter extends RemoteCacheCallback implements
      CacheWriter {

    public RemoteCacheWriter(String desc) {
      super(desc);
    }

    public void beforeUpdate(EntryEvent event) throws CacheWriterException {
    }

    public void beforeCreate(EntryEvent event) throws CacheWriterException {
    }

    public void beforeDestroy(EntryEvent event) throws CacheWriterException {
    }

    public void beforeRegionDestroy(RegionEvent event)
        throws CacheWriterException {
    }
    public void beforeRegionClear(RegionEvent event)
        throws CacheWriterException {
    }
  }

  private static class RemoteCustomExpiry extends RemoteCacheCallback
      implements CustomExpiry, Declarable {

    public RemoteCustomExpiry(String desc) {
      super(desc);
    }
    
    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.CustomExpiry#getExpiry(com.gemstone.gemfire.cache.Region.Entry)
     */
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    /* (non-Javadoc)
     * @see com.gemstone.gemfire.cache.Declarable#init(java.util.Properties)
     */
    public void init(Properties props) {
    }
    
  }
  
  private static class RemoteCacheLoader extends RemoteCacheCallback implements
      CacheLoader {

    public RemoteCacheLoader(String desc) {
      super(desc);
    }

    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }
  }

  private static class RemoteCompressor extends RemoteCacheCallback implements
  Compressor {

    public RemoteCompressor(String desc) {
      super(desc);
    }
    
    public byte[] compress(byte[] input) {
      return null;
    }
    public byte[] decompress(byte[] input) {
      return null;
    }
  }
  
  public EvictionAttributes getEvictionAttributes()
  {
    return this.evictionAttributes;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CustomEvictionAttributes getCustomEvictionAttributes() {
    // TODO: HDFS: no support for custom eviction attributes from remote yet
    return null;
  }

  public boolean getCloningEnabled() {
    // TODO Auto-generated method stub
    return this.cloningEnable;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }
  public String getHDFSStoreName() {
	    return this.hdfsStoreName;
	  }
  public boolean getHDFSWriteOnly() {
    return this.hdfsWriteOnly;
  }
  public boolean isDiskSynchronous() {
    return this.isDiskSynchronous;
  }
  
  public boolean isGatewaySenderEnabled() {
    return this.isGatewaySenderEnabled;
  }

  public Set<String> getGatewaySenderIds() {
    if (this.gatewaySendersDescs == null
        || this.gatewaySendersDescs.length == 0) {
      return Collections.EMPTY_SET;
    }
    else {
      Set<String> senderIds = new HashSet<String>();
      String[] result = new String[this.gatewaySendersDescs.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = new String(this.gatewaySendersDescs[i]);
        senderIds.add(result[i]);
      }
      return senderIds;
    }
  }
  
  public Set<String> getAsyncEventQueueIds() {
    if (this.asyncEventQueueDescs == null
        || this.asyncEventQueueDescs.length == 0) {
      return Collections.EMPTY_SET;
    }
    else {
      Set<String> asyncEventQueues = new HashSet<String>();
      String[] result = new String[this.asyncEventQueueDescs.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = new String(this.asyncEventQueueDescs[i]);
        asyncEventQueues.add(result[i]);
      }
      return asyncEventQueues;
    }

  }
}
