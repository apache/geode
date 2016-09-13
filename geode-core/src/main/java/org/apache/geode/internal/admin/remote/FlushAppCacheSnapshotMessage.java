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
//import org.apache.geode.*;
//import org.apache.geode.internal.*;
import java.io.*;


/**
 * A message to cause a remote application to release any snapshot info it
 * was holding on behalf of a console.
 */
public final class FlushAppCacheSnapshotMessage extends PooledDistributionMessage {

  public static FlushAppCacheSnapshotMessage create() {
    FlushAppCacheSnapshotMessage m = new FlushAppCacheSnapshotMessage();
    return m;
  }


  @Override
  protected void process(DistributionManager dm) {
//     try {
//       AppCacheSnapshotMessage.flushSnapshots(this.getSender());
//     } catch (Exception ex) {
//       LogWriterI18n logger = dm.getLogger();
//       if (logger != null)
//         logger.warning("Failed " + this, ex);
//     }
  }

  public int getDSFID() {
    return FLUSH_APP_CACHE_SNAPSHOT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    super.toData(out);
  }

  @Override
  public void fromData(DataInput in) throws IOException,
      ClassNotFoundException {
    super.fromData(in);
  }

  @Override
  public String toString() {
    return "FlushAppCacheSnapshotMessage from " + this.getSender();
  }
}
