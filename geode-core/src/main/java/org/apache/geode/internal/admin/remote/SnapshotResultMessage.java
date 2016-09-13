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
   
   
package org.apache.geode.internal.admin.remote;

import org.apache.geode.distributed.internal.*;
import org.apache.geode.*;
//import org.apache.geode.internal.*;
import org.apache.geode.internal.admin.*;
import org.apache.geode.cache.*;
import java.io.*;
//import java.util.*;

public final class SnapshotResultMessage extends PooledDistributionMessage
    implements AdminMessageType {
  private CacheSnapshot results;
  private int snapshotId;
  
  public static SnapshotResultMessage create(Region region, int snapshotId)
      throws CacheException {
    SnapshotResultMessage m = new SnapshotResultMessage();
    m.results = new RemoteRegionSnapshot(region);
    m.snapshotId = snapshotId;
    return m;
  }

  @Override
  public void process(DistributionManager dm){
    RemoteGfManagerAgent agent = dm.getAgent();
    if (agent != null){
      agent.enqueueSnapshotResults(this);
    }
  }

  @Override
  public boolean sendViaUDP() {
    return true;
  }

  public CacheSnapshot getSnapshot() {
    return this.results;
  }

  //called by console to verify these results are for the snapshot
  //currently being processed
  public int getSnapshotId() {
    return this.snapshotId;
  }

  public int getDSFID() {
    return SNAPSHOT_RESULT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
    DataSerializer.writeObject(this.results, out);
    out.writeInt(this.snapshotId);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
    this.results = (CacheSnapshot)DataSerializer.readObject(in);
    this.snapshotId = in.readInt();
  }

  
}
