/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache.hdfs.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.hdfs.HDFSEventQueueAttributes;
import com.gemstone.gemfire.internal.lang.ObjectUtils;

/**
 * Implementation of HDFSEventQueueAttributes
 * HDFSEventQueueAttributes represents the attributes of the buffer where events are 
 * accumulated before they are persisted to HDFS  
 * 
 * @author Hemant Bhanawat
 */
public class HDFSEventQueueAttributesImpl implements HDFSEventQueueAttributes, DataSerializable, Cloneable {

  private static final long serialVersionUID = 5052784372168230680L;
  private int maximumQueueMemory;
  private int batchSize;
  private boolean isPersistenceEnabled;
  public String diskStoreName;
  private int batchIntervalMillis;
  private boolean diskSynchronous;
  private int dispatcherThreads;
  
  public HDFSEventQueueAttributesImpl(String diskStoreName,
      int maximumQueueMemory, int batchSize, boolean isPersistenceEnabled,
      int batchIntervalMillis,  boolean diskSynchronous, int dispatcherThreads) {
    this.diskStoreName = diskStoreName;
    this.maximumQueueMemory = maximumQueueMemory;
    this.batchSize = batchSize;
    this.isPersistenceEnabled = isPersistenceEnabled;
    this.batchIntervalMillis = batchIntervalMillis;
    this.diskSynchronous = diskSynchronous;
    this.dispatcherThreads = dispatcherThreads;
  }

  @Override
  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  @Override
  public int getMaximumQueueMemory() {
    return this.maximumQueueMemory;
  }

  @Override
  public int getBatchSizeMB() {
    return this.batchSize;
  }

  @Override
  public boolean isPersistent() {
    return this.isPersistenceEnabled;
  }

  @Override
  public int getBatchTimeInterval() {
    return this.batchIntervalMillis;
  }

  @Override
  public boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }

  @Override
  public String toString()
  {
    StringBuffer s = new StringBuffer();
    return s.append("HDFSEventQueueAttributes@")
      .append(System.identityHashCode(this))
      .append("[maximumQueueMemory=").append(this.maximumQueueMemory)
      .append(";batchSize=").append(this.batchSize)
      .append(";isPersistenceEnabled=").append(this.isPersistenceEnabled)
      .append(";diskStoreName=").append(this.diskStoreName)
      .append(";batchIntervalMillis=").append(this.batchIntervalMillis)
      .append(";diskSynchronous=").append(this.diskSynchronous)
      .append(";dispatcherThreads=").append(this.dispatcherThreads)
      .append("]") .toString();
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeInt(this.maximumQueueMemory);
    out.writeInt(this.batchSize);
    out.writeBoolean(this.isPersistenceEnabled);
    DataSerializer.writeString(this.diskStoreName, out);
    out.writeInt(this.batchIntervalMillis);
    out.writeBoolean(this.diskSynchronous);
    out.writeInt(this.dispatcherThreads);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.maximumQueueMemory = in.readInt();
    this.batchSize = in.readInt();
    this.isPersistenceEnabled = in.readBoolean();
    this.diskStoreName = DataSerializer.readString(in);
    this.batchIntervalMillis = in.readInt();
    this.diskSynchronous = in.readBoolean();
    this.dispatcherThreads = in.readInt();
  }
  
  @Override
  public boolean equals(final Object obj) {
    if (this == obj) { 
      return true;
    }
    
    if (! (obj instanceof HDFSEventQueueAttributes)) {
      return false;
    }
      
    HDFSEventQueueAttributes other = (HDFSEventQueueAttributes) obj;
      
      if (this.maximumQueueMemory != other.getMaximumQueueMemory()
          || this.batchSize != other.getBatchSizeMB()
          || this.isPersistenceEnabled != other.isPersistent()
          || this.batchIntervalMillis != other.getBatchTimeInterval()
          || this.diskSynchronous != other.isDiskSynchronous()
          || this.dispatcherThreads != other.getDispatcherThreads()
              || ObjectUtils.equals(getDiskStoreName(), other.getDiskStoreName())
        ) {
        return false;
        
    }
  
    return true;
  }
  
  @Override
  public Object clone() {
    HDFSEventQueueAttributesImpl other = null;
    try {
      other =
          (HDFSEventQueueAttributesImpl) super.clone();
    } catch (CloneNotSupportedException e) {
    } 
    other.maximumQueueMemory = this.maximumQueueMemory;
    other.batchSize = this.batchSize;
    other.isPersistenceEnabled = this.isPersistenceEnabled;
    other.diskStoreName = this.diskStoreName;
    other.batchIntervalMillis = this.batchIntervalMillis;
    other.diskSynchronous = this.diskSynchronous;
    other.dispatcherThreads = this.dispatcherThreads;
    return other;
  }
  
  @Override
  public int hashCode() {
	assert false : "hashCode not designed";
	return -1;
  }
  
  public HDFSEventQueueAttributesImpl copy() {
    return (HDFSEventQueueAttributesImpl) clone();
  }

	@Override
  public int getDispatcherThreads() {
	  return this.dispatcherThreads;
  }

  
}
