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

package org.apache.geode.internal.admin;

import java.util.ArrayList;
import java.util.List;


/**
 * A <code>CacheCollector</code> is {@linkplain GfManagerAgent#setCacheCollector registered} on a
 * {@link GfManagerAgent} to receive {@link CacheSnapshot}s from other members of the distributed
 * system.
 *
 * @see #takeSnapshot
 */
public class CacheCollector {
  // private SortedMap results;

  /**
   * A list of distributed system members that this collector has not heard back from
   */
  private List notHeardFrom;

  /**
   * A list of distributed system members that this collector has heard back from
   */
  private List heardFrom;

  /**
   * The agent for the distributed system whose distributed cache we are snapshotting
   */
  private final GfManagerAgent systemAgent;

  /**
   * A "view" of the snapshot that is updated as snapshot segments are received.
   */
  private final SnapshotClient view;

  /**
   * A sequence number for the snapshots requested by this VM. We use this sequence number to ignore
   * snapshot segments that are not relevant to the current snapshot
   */
  private static int snapshotCount;

  /**
   * A compound <code>CacheSnapshot</code> that combines all of the Region or Entry instances across
   * a distributed system
   */
  private CacheSnapshot snaps;

  /**
   * Creates a new <code>CacheCollector</code> for gathering snapshots of Cache Region data in a
   * distributed system.
   *
   * @param agent The agent for the distributed system
   * @param view A "view" that is notified as the snapshot is updated
   */
  public CacheCollector(GfManagerAgent agent, SnapshotClient view) {
    this.view = view;
    this.systemAgent = agent;
    systemAgent.setCacheCollector(this);
  }

  /**
   * Initiates a snapshot of the all of the Cache regions in a distributed system.
   *
   * @see org.apache.geode.internal.admin.ApplicationVM#takeRegionSnapshot(String, int)
   */
  public synchronized void takeSnapshot(String regionName) {
    flush();
    ApplicationVM[] apps = systemAgent.listApplications();

    for (int j = 0; j < apps.length; j++) {
      notHeardFrom.add(apps[j]);
      apps[j].takeRegionSnapshot(regionName, snapshotCount);
    }
  }

  /**
   * Flushes results from previous snapshots and resets the snapshot state.
   */
  public synchronized void flush() {
    snapshotCount++;
    // if (heardFrom != null && notHeardFrom != null) {
    // forEachFlush(heardFrom.iterator());
    // forEachFlush(notHeardFrom.iterator());
    // }
    clear();
  }

  // private void forEachFlush(Iterator iter) {
  // while (iter.hasNext()) {
  // Object member = iter.next();
  // try {
  // ((ApplicationVM)member).flushSnapshots();
  // } catch (RuntimeAdminException ignore) {}
  // }
  // }

  /**
   * Closes this <code>CacheCollector</code> so it no longer processes snapshot fragments.
   */
  public void close() {
    flush();
    this.systemAgent.setCacheCollector(null);
  }

  /**
   * Resets the internal state of this <code>CacheCollector</code>
   */
  private synchronized void clear() {
    // this.results = new TreeMap(new SnapshotNameComparator());
    snaps = null;
    this.notHeardFrom = new ArrayList();
    this.heardFrom = new ArrayList();
  }

  /**
   * Amalgamates a segment of a cache snapshot into the compound snapshot.
   *
   * @param update A newly-received snapshot
   * @param poster The distributed system member that sent the <code>CacheSnapshot</code>.
   *
   * @return The compound snapshot containing the newly-amalgamated <code>update</code>.
   */
  private CacheSnapshot updateResultSet(CacheSnapshot update, GemFireVM poster) {
    noteResponse(poster);

    if (update instanceof EntrySnapshot) {
      if (snaps instanceof CompoundRegionSnapshot) {
        throw new IllegalStateException(
            "Unable to mix region and entry snapshots in CacheCollector.");
      }
      if (snaps == null) {
        snaps = new CompoundEntrySnapshot(update.getName());
      }
      ((CompoundEntrySnapshot) snaps).addCache(poster, (EntrySnapshot) update);
    } else if (update instanceof RegionSnapshot) {
      if (snaps instanceof CompoundEntrySnapshot) {
        throw new IllegalStateException(
            "Unable to mix region and entry snapshots in CacheCollector.");
      }
      if (snaps == null) {
        snaps = new CompoundRegionSnapshot(update.getName().toString());
      }
      ((CompoundRegionSnapshot) snaps).addCache(poster, (RegionSnapshot) update);
    }

    // Set keys = results.keySet();
    // for (int i=0; i<update.length; i++) {
    // CacheSnapshot temp = update[i];
    // Object name = temp.getName();
    // if (keys.contains(name)) {
    // CacheSnapshot cs = (CacheSnapshot)results.get(name);
    // if (cs instanceof CompoundEntrySnapshot) {
    // ((CompoundEntrySnapshot)cs).addCache(poster, (EntrySnapshot)temp);
    // } else if (cs instanceof CompoundRegionSnapshot) {
    // ((CompoundRegionSnapshot)cs).addCache(poster, (RegionSnapshot)temp);73
    // }
    // } else {
    // if (temp instanceof EntrySnapshot) {
    // EntrySnapshot entry = (EntrySnapshot)temp;
    // CompoundEntrySnapshot snap = new CompoundEntrySnapshot(name);
    // snap.addCache(poster, entry);
    // results.put(name, snap);
    // } else if (temp instanceof RegionSnapshot) {
    // RegionSnapshot region = (RegionSnapshot)temp;
    // CompoundRegionSnapshot snap = new CompoundRegionSnapshot((String)name);
    // snap.addCache(poster, region);
    // results.put(name, snap);
    // }
    // }
    // }

    // return results.values();
    return snaps;
  }

  /**
   * Notes that we got a response from a given member of the distributed system.
   */
  private void noteResponse(GemFireVM responder) {
    if (notHeardFrom.remove(responder)) {
      heardFrom.add(responder);
    }
  }

  // private ApplicationProcess findResultPoster(GfManager parent, long connId) {
  // for (Iterator iter = notHeardFrom.iterator(); iter.hasNext(); ) {
  // ApplicationProcess app = (ApplicationProcess)iter.next();
  // if (app.getSystemManager().equals(parent) && app.getId() == connId) {
  // return app;
  // }
  // }
  // for (Iterator iter = heardFrom.iterator(); iter.hasNext(); ) {
  // ApplicationProcess app = (ApplicationProcess)iter.next();
  // if (app.getSystemManager().equals(parent) && app.getId() == connId) {
  // return app;
  // }
  // }
  // return null; //couldn't find the member...
  // }

  /**
   * This method is called as result segments come in. It updates the superset of all results and
   * calls back to the view.
   *
   * @param snap The snapshot segment of the cache
   * @param member The member of the distributed system that hosts the cache segment.
   * @param snapshotId The sequence number of the snapshot. If the sequence number is not for the
   *        current snapshot, then the segment is ignored.
   */
  public synchronized void resultsReturned(CacheSnapshot snap, GemFireVM member, int snapshotId) {
    if (snapshotId == CacheCollector.snapshotCount) {
      // protect against stragglers from previous snapshots

      if (snap == null) {
        noteResponse(member);
      } else {
        view.updateSnapshot(updateResultSet(snap, member), new ArrayList(heardFrom));
      }
      // ApplicationProcess cacheMember = findResultPoster(manager, appConnId);
      // if (cacheMember != null) {
      // if (snaps.length == 0) {
      // noteResponse(cacheMember);
      // } else {
      // view.updateSnapshot(updateResultSet(snaps, cacheMember));
      // }
      // }
    }
  }

  //// inner classes ///////////////////////

  // private class SnapshotNameComparator implements Comparator {
  // public int compare(Object a, Object b) {
  // int hashA = a.hashCode();
  // int hashB = b.hashCode();
  // if (hashA == hashB) {
  // return 0;
  // } else {
  // return (hashA > hashB) ? 1 : -1;
  // }
  // }
  // }

}
