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

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.EntryEvent;
import com.gemstone.gemfire.internal.InternalDataSerializer;
import com.gemstone.gemfire.internal.cache.EntryEventImpl;
import com.gemstone.gemfire.internal.cache.EnumListenerEvent;
import com.gemstone.gemfire.internal.cache.LocalRegion;
import com.gemstone.gemfire.internal.cache.lru.Sizeable;
import com.gemstone.gemfire.internal.cache.tier.sockets.CacheServerHelper;
import com.gemstone.gemfire.internal.cache.versions.VersionTag;
import com.gemstone.gemfire.internal.cache.wan.GatewaySenderEventImpl;
import com.gemstone.gemfire.internal.offheap.StoredObject;
import com.gemstone.gemfire.internal.offheap.annotations.Retained;
import com.gemstone.gemfire.internal.util.BlobHelper;


/**
 * Gateway event extended for HDFS functionality 
 *
 * @author Hemant Bhanawat
 */
public class HDFSGatewayEventImpl extends GatewaySenderEventImpl {
  
  private static final long serialVersionUID = 4642852957292192406L;
  protected transient boolean keyIsSerialized = false;
  protected byte[] serializedKey = null; 
  protected VersionTag versionTag; 
  
  public HDFSGatewayEventImpl(){
  }
  
  @Retained
  public HDFSGatewayEventImpl(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue)
      throws IOException {
    super(operation, event, substituteValue);
    initializeHDFSGatewayEventObject(event);
  }

  @Retained
  public HDFSGatewayEventImpl(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue, boolean initialize, int bucketId) throws IOException {
    super(operation, event,substituteValue, initialize, bucketId);
    initializeHDFSGatewayEventObject(event);
  }

  @Retained
  public HDFSGatewayEventImpl(EnumListenerEvent operation, EntryEvent event,
      Object substituteValue, boolean initialize) throws IOException {
    super(operation, event, substituteValue, initialize);
    initializeHDFSGatewayEventObject(event);
  }

  protected HDFSGatewayEventImpl(HDFSGatewayEventImpl offHeapEvent) {
    super(offHeapEvent);
    this.keyIsSerialized = offHeapEvent.keyIsSerialized;
    this.serializedKey = offHeapEvent.serializedKey;
    this.versionTag = offHeapEvent.versionTag;
  }
  
  @Override
  protected GatewaySenderEventImpl makeCopy() {
    return new HDFSGatewayEventImpl(this);
  }

  private void initializeHDFSGatewayEventObject(EntryEvent event)
      throws IOException {

    serializeKey();
    versionTag = ((EntryEventImpl)event).getVersionTag();
    if (versionTag != null && versionTag.getMemberID() == null) {
      versionTag.setMemberID(((LocalRegion)getRegion()).getVersionMember());
    }
  }

  private void serializeKey() throws IOException {
    if (!keyIsSerialized && isInitialized())
    {
      this.serializedKey = CacheServerHelper.serialize(this.key);
      keyIsSerialized = true;
    } 
  }
  /**MergeGemXDHDFSToGFE This function needs to enabled if similar functionality is added to gatewaysendereventimpl*/
  /*@Override
  protected StoredObject obtainOffHeapValueBasedOnOp(EntryEventImpl event,
      boolean hasNonWanDispatcher) {
    return  event.getOffHeapNewValue();
  }*/
  
  /**MergeGemXDHDFSToGFE This function needs to enabled if similar functionality is added to gatewaysendereventimpl*/
  /*@Override
  protected Object obtainHeapValueBasedOnOp(EntryEventImpl event,
      boolean hasNonWanDispatcher) {
    return   event.getRawNewValue(shouldApplyDelta());
  }*/
  
  @Override
  protected boolean shouldApplyDelta() {
    return true;
  }

  
  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.versionTag, out);
    
  }
  
  @Override
  protected void serializeKey(DataOutput out) throws IOException {
    DataSerializer.writeByteArray((byte[])this.serializedKey, out);
  }
  
  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    super.fromData(in);
    this.versionTag = (VersionTag)DataSerializer.readObject(in);
  }
  
  @Override
  protected void deserializeKey(DataInput in) throws IOException,
    ClassNotFoundException {
    this.serializedKey = DataSerializer.readByteArray(in);
    this.key = BlobHelper.deserializeBlob(this.serializedKey,
        InternalDataSerializer.getVersionForDataStreamOrNull(in), null);
    keyIsSerialized = true;
  }

  @Override
  public int getDSFID() {
    
    return HDFS_GATEWAY_EVENT_IMPL;
  }
  public byte[] getSerializedKey() {
    
    return this.serializedKey;
  }
  
  public VersionTag getVersionTag() {
    
    return this.versionTag;
  }
  
  /**
   * Returns the size on HDFS of this event  
   * @param writeOnly
   */
  public int getSizeOnHDFSInBytes(boolean writeOnly) {
  
    if (writeOnly)
      return UnsortedHDFSQueuePersistedEvent.getSizeInBytes(this.serializedKey.length,  
          getSerializedValueSize(), this.versionTag);
    else
      return SortedHDFSQueuePersistedEvent.getSizeInBytes(this.serializedKey.length,  
          getSerializedValueSize(), this.versionTag);
  
  }
}
