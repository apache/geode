/*=========================================================================
 * Copyright Copyright (c) 2000-2011 VMware, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * more patents listed at http://www.vmware.com/go/patents.
 * $Id: QueryObserverAdapter.java,v 1.2 2005/02/01 17:19:20 vaibhav Exp $
 *=========================================================================
 */
package com.gemstone.gemfire.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.internal.DataSerializableFixedID;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;


public class PRQueryTraceInfo implements DataSerializableFixedID {

  public String indexesUsed = "";
  //Set and used by the pr query gathering side for logging purposes
  private InternalDistributedMember sender;
  private float timeInMillis;
  private int numResults;
  
  public PRQueryTraceInfo() {
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    out.writeFloat(timeInMillis);
    out.writeInt(numResults);
    DataSerializer.writeString(indexesUsed, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    timeInMillis = in.readFloat();
    numResults = in.readInt();
    indexesUsed = DataSerializer.readString(in);
  }
  

  @Override
  public Version[] getSerializationVersions() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getDSFID() {
    return PR_QUERY_TRACE_INFO;
  }
  
  public int calculateNumberOfResults(Collection resultCollector) {
    int traceSize = 0;
    Iterator<Collection> iterator = resultCollector.iterator();
    while (iterator.hasNext()) {
      Collection collection = iterator.next();
      traceSize += collection.size();
    }
    return traceSize;
  }

  public String createLogLine(DistributedMember me) {
    if (sender.equals(me)) {
      return LocalizedStrings.PartitionedRegion_QUERY_TRACE_LOCAL_NODE_LOG.toLocalizedString(sender, timeInMillis, numResults, indexesUsed);
    }
    else {
      return LocalizedStrings.PartitionedRegion_QUERY_TRACE_REMOTE_NODE_LOG.toLocalizedString( sender, timeInMillis, numResults, indexesUsed);
    }
  }
  
  public float getTimeInMillis() {
    return timeInMillis;
  }
  
  public void setTimeInMillis(float timeInMillis) {
    this.timeInMillis = timeInMillis;
  }
  
  public void setSender(InternalDistributedMember sender) {
    this.sender = sender;
  }
  
  public void setNumResults(int numResults) {
    this.numResults = numResults;
  }
  
  public void setIndexesUsed(String indexesUsed) {
    this.indexesUsed = indexesUsed;
  }
  
}
