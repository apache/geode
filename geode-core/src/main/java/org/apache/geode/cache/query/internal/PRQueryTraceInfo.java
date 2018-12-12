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
package org.apache.geode.cache.query.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.DataSerializableFixedID;
import org.apache.geode.internal.Version;


public class PRQueryTraceInfo implements DataSerializableFixedID {

  public String indexesUsed = "";
  // Set and used by the pr query gathering side for logging purposes
  private InternalDistributedMember sender;
  private float timeInMillis;
  private int numResults;

  public PRQueryTraceInfo() {}

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
      return String.format("Local %s took %sms and returned %s results; %s", sender,
          timeInMillis, numResults, indexesUsed);
    } else {
      return String.format("Remote %s took %sms and returned %s results; %s",
          sender, timeInMillis, numResults, indexesUsed);
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
