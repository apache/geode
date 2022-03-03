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
import java.io.IOException;

import org.apache.geode.GemFireCacheException;
import org.apache.geode.cache.CacheException;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;

public class AppCacheSnapshotMessage extends RegionAdminMessage {
  // private int numResults;
  // private static Map consolesToSnapshots = new HashMap();
  private int snapshotId;

  public static AppCacheSnapshotMessage create(String regionName, int snapshotId) {
    AppCacheSnapshotMessage m = new AppCacheSnapshotMessage();
    m.setRegionName(regionName);
    // m.numResults = numResults;
    m.snapshotId = snapshotId;
    return m;
  }

  @Override
  protected void process(ClusterDistributionManager dm) {
    Region r = getRegion(dm.getSystem());
    if (r != null) {
      try {
        // LinkedList entries = getEntriesForRegion(r, this.getSender());
        // new ResponseThread(this.getSender(), numResults, dm, this.snapshotId).start();
        SnapshotResultMessage m = SnapshotResultMessage.create(r, snapshotId);
        m.setRecipient(getSender());
        dm.putOutgoing(m);
      } catch (CacheException ex) {
        throw new GemFireCacheException(ex);
      }
    }
  }

  // public static synchronized void flushSnapshots(Serializable consoleAddr) {
  // consolesToSnapshots.remove(consoleAddr);
  // System.gc();
  // }

  // private static synchronized LinkedList getEntriesForRegion(Region r, Serializable recipient)
  // throws CacheException {
  // Object obj = consolesToSnapshots.get(recipient);
  // if (obj == null) {
  // boolean statsEnabled = r.getAttributes().getStatisticsEnabled();
  // LinkedList snaps = new LinkedList();

  // synchronized(r) {
  // Set entries = r.entries(false);
  // Set subRegions = r.subregions(false);
  // snaps.addLast(new RemoteRegionSnapshot(r)); //add region itself

  // for (Iterator iter = subRegions.iterator(); iter.hasNext(); ) {
  // snaps.addLast(new RemoteRegionSnapshot((Region)iter.next()));
  // }

  // for (Iterator iter = entries.iterator(); iter.hasNext(); ) {
  // snaps.addLast(new RemoteEntrySnapshot((Region.Entry)iter.next(), statsEnabled));
  // }
  // }

  // consolesToSnapshots.put(recipient, snaps);
  // return snaps;
  // } else {
  // return (LinkedList)obj;
  // }
  // }

  // private static synchronized CacheSnapshot[] extractElements(int numElements,
  // Serializable console) {
  // LinkedList ll = (LinkedList)consolesToSnapshots.get(console);
  // if (ll == null) {
  // return new CacheSnapshot[0];
  // }
  // numElements = (numElements > ll.size()) ? ll.size() : numElements;
  // CacheSnapshot[] snaps = new CacheSnapshot[numElements];
  // for (int i=0; i<numElements; i++) {
  // snaps[i] = (CacheSnapshot)ll.removeFirst();
  // }
  // return snaps;
  // }

  @Override
  public int getDSFID() {
    return APP_CACHE_SNAPSHOT_MESSAGE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    super.toData(out, context);
    // out.writeInt(numResults);
    out.writeInt(snapshotId);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    super.fromData(in, context);
    // this.numResults = in.readInt();
    snapshotId = in.readInt();
  }

  @Override
  public String toString() {
    return "AppCacheSnapshotMessage from " + getSender();
  }

  /// inner classes /////////////////////////////////////////////////
  // private static class ResponseThread extends Thread {
  // private final Serializable console;
  // private final int resultSegSize;
  // private final DistributionManager dm;
  // private final int snapshotId;

  // public ResponseThread(Serializable console, int resultSegmentSize,
  // DistributionManager dm, int snapshotId) {
  // this.console = console;
  // this.resultSegSize = resultSegmentSize;
  // this.dm = dm;
  // this.snapshotId = snapshotId;
  // setDaemon(true);
  // }

  // public void run() {
  // //loop while results exist for console and send them back
  // // a chunk at a time. Send a zero length array to signal we're done
  // while (true) {
  // CacheSnapshot[] resultChunk = extractElements(resultSegSize, console);
  // SnapshotResultMessage m = SnapshotResultMessage.create(resultChunk, snapshotId);
  // m.setRecipient(console);
  // dm.putOutgoing(m);
  // if (resultChunk.length == 0) {
  // break;
  // } else {
  // try {
  // sleep(50);
  // } catch (InterruptedException ignore) {}
  // }
  // }
  // }
  // }
}
