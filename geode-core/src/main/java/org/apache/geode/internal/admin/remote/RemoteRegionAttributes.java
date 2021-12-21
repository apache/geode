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
package org.apache.geode.internal.admin.remote;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.cache.CacheListener;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.CustomExpiry;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.DiskWriteAttributes;
import org.apache.geode.cache.EntryEvent;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.ExpirationAttributes;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.cache.MembershipAttributes;
import org.apache.geode.cache.MirrorType;
import org.apache.geode.cache.PartitionAttributes;
import org.apache.geode.cache.Region.Entry;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.RegionEvent;
import org.apache.geode.cache.Scope;
import org.apache.geode.cache.SubscriptionAttributes;
import org.apache.geode.compression.Compressor;
import org.apache.geode.internal.cache.EvictionAttributesImpl;

/**
 * Provides an implementation of RegionAttributes that can be used from a VM remote from the vm that
 * created the cache.
 */
public class RemoteRegionAttributes implements RegionAttributes, DataSerializable {

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
  private String compressorDesc;
  private boolean offHeap;

  /**
   * constructs a new default RemoteRegionAttributes.
   */
  public RemoteRegionAttributes(RegionAttributes attr) {
    cacheLoaderDesc = getDesc(attr.getCacheLoader());
    cacheWriterDesc = getDesc(attr.getCacheWriter());
    cacheListenerDescs = getDescs(attr.getCacheListeners());
    keyConstraint = attr.getKeyConstraint();
    valueConstraint = attr.getValueConstraint();
    rTtl = attr.getRegionTimeToLive();
    rIdleTimeout = attr.getRegionIdleTimeout();
    eTtl = attr.getEntryTimeToLive();
    customEttlDesc = getDesc(attr.getCustomEntryTimeToLive());
    eIdleTimeout = attr.getEntryIdleTimeout();
    customEIdleDesc = getDesc(attr.getCustomEntryIdleTimeout());
    dataPolicy = attr.getDataPolicy();
    scope = attr.getScope();
    statsEnabled = attr.getStatisticsEnabled();
    ignoreJTA = attr.getIgnoreJTA();
    concurrencyLevel = attr.getConcurrencyLevel();
    concurrencyChecksEnabled = attr.getConcurrencyChecksEnabled();
    loadFactor = attr.getLoadFactor();
    initialCapacity = attr.getInitialCapacity();
    earlyAck = attr.getEarlyAck();
    multicastEnabled = attr.getMulticastEnabled();
    // this.enableGateway = attr.getEnableGateway();
    // this.gatewayHubId = attr.getGatewayHubId();
    enableSubscriptionConflation = attr.getEnableSubscriptionConflation();
    publisher = attr.getPublisher();
    enableAsyncConflation = attr.getEnableAsyncConflation();
    diskStoreName = attr.getDiskStoreName();
    if (diskStoreName == null) {
      diskWriteAttributes = attr.getDiskWriteAttributes();
      diskDirs = attr.getDiskDirs();
      diskSizes = attr.getDiskDirSizes();
    } else {
      diskWriteAttributes = null;
      diskDirs = null;
      diskSizes = null;
    }
    partitionAttributes = attr.getPartitionAttributes();
    membershipAttributes = attr.getMembershipAttributes();
    subscriptionAttributes = attr.getSubscriptionAttributes();
    cloningEnable = attr.getCloningEnabled();
    poolName = attr.getPoolName();
    isDiskSynchronous = attr.isDiskSynchronous();
    gatewaySendersDescs = getDescs(attr.getGatewaySenderIds().toArray());
    asyncEventQueueDescs = getDescs(attr.getAsyncEventQueueIds().toArray());
    compressorDesc = getDesc(attr.getCompressor());
    offHeap = attr.getOffHeap();
  }

  /**
   * For use only by DataExternalizable mechanism
   */
  public RemoteRegionAttributes() {}

  @Override
  public CacheLoader getCacheLoader() {
    return cacheLoaderDesc.equals("") ? null : new RemoteCacheLoader(cacheLoaderDesc);
  }

  @Override
  public CacheWriter getCacheWriter() {
    return cacheWriterDesc.equals("") ? null : new RemoteCacheWriter(cacheWriterDesc);
  }

  @Override
  public Class getKeyConstraint() {
    return keyConstraint;
  }

  @Override
  public Class getValueConstraint() {
    return valueConstraint;
  }

  @Override
  public ExpirationAttributes getRegionTimeToLive() {
    return rTtl;
  }

  @Override
  public ExpirationAttributes getRegionIdleTimeout() {
    return rIdleTimeout;
  }

  @Override
  public ExpirationAttributes getEntryTimeToLive() {
    return eTtl;
  }

  @Override
  public CustomExpiry getCustomEntryTimeToLive() {
    return customEttlDesc.equals("") ? null : new RemoteCustomExpiry(customEttlDesc);
  }

  @Override
  public ExpirationAttributes getEntryIdleTimeout() {
    return eIdleTimeout;
  }

  @Override
  public CustomExpiry getCustomEntryIdleTimeout() {
    return customEIdleDesc.equals("") ? null : new RemoteCustomExpiry(customEIdleDesc);
  }


  @Override
  public String getPoolName() {
    return poolName;
  }

  @Override
  public Scope getScope() {
    return scope;
  }

  @Override
  public CacheListener getCacheListener() {
    CacheListener[] listeners = getCacheListeners();
    if (listeners.length == 0) {
      return null;
    } else if (listeners.length == 1) {
      return listeners[0];
    } else {
      throw new IllegalStateException(
          "More than one cache listener exists.");
    }
  }

  @Immutable
  private static final CacheListener[] EMPTY_LISTENERS = new CacheListener[0];

  @Override
  public CacheListener[] getCacheListeners() {
    if (cacheListenerDescs == null || cacheListenerDescs.length == 0) {
      return EMPTY_LISTENERS;
    } else {
      CacheListener[] result = new CacheListener[cacheListenerDescs.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = new RemoteCacheListener(cacheListenerDescs[i]);
      }
      return result;
    }
  }


  @Override
  public int getInitialCapacity() {
    return initialCapacity;
  }

  @Override
  public float getLoadFactor() {
    return loadFactor;
  }

  @Override
  public int getConcurrencyLevel() {
    return concurrencyLevel;
  }

  @Override
  public boolean getConcurrencyChecksEnabled() {
    return concurrencyChecksEnabled;
  }

  @Override
  public boolean getStatisticsEnabled() {
    return statsEnabled;
  }

  @Override
  public boolean getIgnoreJTA() {
    return ignoreJTA;
  }

  @Override
  public boolean isLockGrantor() {
    return isLockGrantor;
  }

  @Override
  public boolean getPersistBackup() {
    return getDataPolicy().withPersistence();
  }

  @Override
  public boolean getEarlyAck() {
    return earlyAck;
  }

  @Override
  public boolean getMulticastEnabled() {
    return multicastEnabled;
  }

  public boolean getEnableGateway() {
    return enableGateway;
  }

  public boolean getEnableWAN() { // deprecated in 5.0
    return enableGateway;
  }

  public String getGatewayHubId() {
    return gatewayHubId;
  }

  /*
   * @deprecated as of prPersistSprint1
   */
  @Override
  @Deprecated
  public boolean getPublisher() {
    return publisher;
  }

  @Override
  public boolean getEnableConflation() { // deprecated in 5.0
    return getEnableSubscriptionConflation();
  }

  @Override
  public boolean getEnableBridgeConflation() { // deprecated in 5.7
    return getEnableSubscriptionConflation();
  }

  @Override
  public boolean getEnableSubscriptionConflation() {
    return enableSubscriptionConflation;
  }

  @Override
  public boolean getEnableAsyncConflation() {
    return enableAsyncConflation;
  }

  @Override
  public DiskWriteAttributes getDiskWriteAttributes() {
    return diskWriteAttributes;
  }

  @Override
  public File[] getDiskDirs() {
    return diskDirs;
  }

  @Override
  public int[] getDiskDirSizes() {
    return diskSizes;
  }

  @Override
  public MirrorType getMirrorType() {
    // checkReadiness();
    if (dataPolicy.isNormal() || dataPolicy.isPreloaded() || dataPolicy.isEmpty()
        || dataPolicy.withPartitioning()) {
      return MirrorType.NONE;
    } else if (dataPolicy.withReplication()) {
      return MirrorType.KEYS_VALUES;
    } else {
      throw new IllegalStateException(
          String.format("No mirror type corresponds to data policy %s",
              dataPolicy));
    }
  }

  @Override
  public DataPolicy getDataPolicy() {
    // checkReadiness();
    return dataPolicy;
  }

  @Override
  public PartitionAttributes getPartitionAttributes() {
    return partitionAttributes;
  }

  @Override
  public MembershipAttributes getMembershipAttributes() {
    return membershipAttributes;
  }

  @Override
  public SubscriptionAttributes getSubscriptionAttributes() {
    return subscriptionAttributes;
  }

  @Override
  public Compressor getCompressor() {
    return compressorDesc.equals("") ? null : new RemoteCompressor(compressorDesc);
  }

  @Override
  public boolean getOffHeap() {
    return offHeap;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(cacheLoaderDesc, out);
    DataSerializer.writeString(cacheWriterDesc, out);
    DataSerializer.writeStringArray(cacheListenerDescs, out);
    DataSerializer.writeString(capacityControllerDesc, out);
    DataSerializer.writeObject(keyConstraint, out);
    DataSerializer.writeObject(valueConstraint, out);
    DataSerializer.writeObject(rTtl, out);
    DataSerializer.writeObject(rIdleTimeout, out);
    DataSerializer.writeObject(eTtl, out);
    DataSerializer.writeString(customEttlDesc, out);
    DataSerializer.writeObject(eIdleTimeout, out);
    DataSerializer.writeString(customEIdleDesc, out);
    DataSerializer.writeObject(dataPolicy, out);
    DataSerializer.writeObject(scope, out);
    out.writeBoolean(statsEnabled);
    out.writeBoolean(ignoreJTA);
    out.writeInt(concurrencyLevel);
    out.writeFloat(loadFactor);
    out.writeInt(initialCapacity);
    out.writeBoolean(earlyAck);
    out.writeBoolean(multicastEnabled);
    out.writeBoolean(enableSubscriptionConflation);
    out.writeBoolean(publisher);
    out.writeBoolean(enableAsyncConflation);

    DataSerializer.writeObject(diskWriteAttributes, out);
    DataSerializer.writeObject(diskDirs, out);
    DataSerializer.writeObject(diskSizes, out);
    out.writeBoolean(indexMaintenanceSynchronous);
    DataSerializer.writeObject(partitionAttributes, out);
    DataSerializer.writeObject(membershipAttributes, out);
    DataSerializer.writeObject(subscriptionAttributes, out);
    DataSerializer.writeObject(evictionAttributes, out);
    out.writeBoolean(cloningEnable);
    DataSerializer.writeString(diskStoreName, out);
    out.writeBoolean(isDiskSynchronous);
    DataSerializer.writeStringArray(gatewaySendersDescs, out);
    out.writeBoolean(isGatewaySenderEnabled);

    out.writeBoolean(concurrencyChecksEnabled);

    DataSerializer.writeString(compressorDesc, out);
    out.writeBoolean(offHeap);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    cacheLoaderDesc = DataSerializer.readString(in);
    cacheWriterDesc = DataSerializer.readString(in);
    cacheListenerDescs = DataSerializer.readStringArray(in);
    capacityControllerDesc = DataSerializer.readString(in);
    keyConstraint = DataSerializer.readObject(in);
    valueConstraint = DataSerializer.readObject(in);
    rTtl = DataSerializer.readObject(in);
    rIdleTimeout = DataSerializer.readObject(in);
    eTtl = DataSerializer.readObject(in);
    customEttlDesc = DataSerializer.readString(in);
    eIdleTimeout = DataSerializer.readObject(in);
    customEIdleDesc = DataSerializer.readString(in);
    dataPolicy = DataSerializer.readObject(in);
    scope = DataSerializer.readObject(in);
    statsEnabled = in.readBoolean();
    ignoreJTA = in.readBoolean();
    concurrencyLevel = in.readInt();
    loadFactor = in.readFloat();
    initialCapacity = in.readInt();
    earlyAck = in.readBoolean();
    multicastEnabled = in.readBoolean();
    enableSubscriptionConflation = in.readBoolean();
    publisher = in.readBoolean();
    enableAsyncConflation = in.readBoolean();

    diskWriteAttributes = DataSerializer.readObject(in);
    diskDirs = DataSerializer.readObject(in);
    diskSizes = DataSerializer.readObject(in);
    indexMaintenanceSynchronous = in.readBoolean();
    partitionAttributes = DataSerializer.readObject(in);
    membershipAttributes = DataSerializer.readObject(in);
    subscriptionAttributes = DataSerializer.readObject(in);
    evictionAttributes = DataSerializer.readObject(in);
    cloningEnable = in.readBoolean();
    diskStoreName = DataSerializer.readString(in);
    isDiskSynchronous = in.readBoolean();
    gatewaySendersDescs = DataSerializer.readStringArray(in);
    isGatewaySenderEnabled = in.readBoolean();
    concurrencyChecksEnabled = in.readBoolean();

    compressorDesc = DataSerializer.readString(in);
    offHeap = in.readBoolean();
  }

  private String[] getDescs(Object[] l) {
    if (l == null) {
      return new String[0];
    } else {
      String[] result = new String[l.length];
      for (int i = 0; i < l.length; i++) {
        result[i] = getDesc(l[i]);
      }
      return result;
    }
  }

  private String getDesc(Object o) {
    if (o == null) {
      return "";
    } else if (o instanceof RemoteCacheCallback) {
      return o.toString();
    } else {
      return o.getClass().getName();
    }
  }

  @Override
  public boolean getIndexMaintenanceSynchronous() {
    return indexMaintenanceSynchronous;
  }

  /**
   * A remote representation of a cache callback
   */
  private abstract static class RemoteCacheCallback implements CacheCallback {

    /** The description of this callback */
    private final String desc;

    /**
     * Creates a new <code>RemoteCacheCallback</code> with the given description.
     */
    protected RemoteCacheCallback(String desc) {
      this.desc = desc;
    }

    @Override
    public String toString() {
      return desc;
    }

    @Override
    public void close() {}
  }

  private static class RemoteCacheListener extends RemoteCacheCallback implements CacheListener {

    public RemoteCacheListener(String desc) {
      super(desc);
    }

    @Override
    public void afterCreate(EntryEvent event) {}

    @Override
    public void afterUpdate(EntryEvent event) {}

    @Override
    public void afterInvalidate(EntryEvent event) {}

    @Override
    public void afterDestroy(EntryEvent event) {}

    @Override
    public void afterRegionInvalidate(RegionEvent event) {}

    @Override
    public void afterRegionDestroy(RegionEvent event) {}

    @Override
    public void afterRegionClear(RegionEvent event) {}

    @Override
    public void afterRegionCreate(RegionEvent event) {}

    @Override
    public void afterRegionLive(RegionEvent event) {}
  }

  private static class RemoteCacheWriter extends RemoteCacheCallback implements CacheWriter {

    public RemoteCacheWriter(String desc) {
      super(desc);
    }

    @Override
    public void beforeUpdate(EntryEvent event) throws CacheWriterException {}

    @Override
    public void beforeCreate(EntryEvent event) throws CacheWriterException {}

    @Override
    public void beforeDestroy(EntryEvent event) throws CacheWriterException {}

    @Override
    public void beforeRegionDestroy(RegionEvent event) throws CacheWriterException {}

    @Override
    public void beforeRegionClear(RegionEvent event) throws CacheWriterException {}
  }

  private static class RemoteCustomExpiry extends RemoteCacheCallback
      implements CustomExpiry, Declarable {

    public RemoteCustomExpiry(String desc) {
      super(desc);
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.CustomExpiry#getExpiry(org.apache.geode.cache.Region.Entry)
     */
    @Override
    public ExpirationAttributes getExpiry(Entry entry) {
      return null;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
     */
    @Override
    public void init(Properties props) {}

  }

  private static class RemoteCacheLoader extends RemoteCacheCallback implements CacheLoader {

    public RemoteCacheLoader(String desc) {
      super(desc);
    }

    @Override
    public Object load(LoaderHelper helper) throws CacheLoaderException {
      return null;
    }
  }

  private static class RemoteCompressor extends RemoteCacheCallback implements Compressor {

    public RemoteCompressor(String desc) {
      super(desc);
    }

    @Override
    public byte[] compress(byte[] input) {
      return null;
    }

    @Override
    public byte[] decompress(byte[] input) {
      return null;
    }
  }

  @Override
  public EvictionAttributes getEvictionAttributes() {
    return evictionAttributes;
  }

  @Override
  public boolean getCloningEnabled() {
    // TODO Auto-generated method stub
    return cloningEnable;
  }

  @Override
  public String getDiskStoreName() {
    return diskStoreName;
  }

  @Override
  public boolean isDiskSynchronous() {
    return isDiskSynchronous;
  }

  public boolean isGatewaySenderEnabled() {
    return isGatewaySenderEnabled;
  }

  @Override
  public Set<String> getGatewaySenderIds() {
    if (gatewaySendersDescs == null || gatewaySendersDescs.length == 0) {
      return Collections.EMPTY_SET;
    } else {
      Set<String> senderIds = new HashSet<String>();
      String[] result = new String[gatewaySendersDescs.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = gatewaySendersDescs[i];
        senderIds.add(result[i]);
      }
      return senderIds;
    }
  }

  @Override
  public Set<String> getAsyncEventQueueIds() {
    if (asyncEventQueueDescs == null || asyncEventQueueDescs.length == 0) {
      return Collections.EMPTY_SET;
    } else {
      Set<String> asyncEventQueues = new HashSet<String>();
      String[] result = new String[asyncEventQueueDescs.length];
      for (int i = 0; i < result.length; i++) {
        result[i] = asyncEventQueueDescs[i];
        asyncEventQueues.add(result[i]);
      }
      return asyncEventQueues;
    }

  }
}
