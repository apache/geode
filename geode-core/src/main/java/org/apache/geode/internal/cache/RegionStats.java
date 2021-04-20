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
package org.apache.geode.internal.cache;


public interface RegionStats {

  void incReliableQueuedOps(int inc);

  void incReliableQueueSize(int inc);

  void incReliableQueueMax(int inc);

  void incReliableRegions(int inc);

  void incReliableRegionsMissing(int inc);

  void incReliableRegionsQueuing(int inc);

  void incReliableRegionsMissingFullAccess(int inc);

  void incReliableRegionsMissingLimitedAccess(int inc);

  void incReliableRegionsMissingNoAccess(int inc);

  void incQueuedEvents(int inc);

  long startLoad();

  void endLoad(long start);

  long startNetload();

  void endNetload(long start);

  long startNetsearch();

  void endNetsearch(long start);

  long startCacheWriterCall();

  void endCacheWriterCall(long start);

  long startCacheListenerCall();

  void endCacheListenerCall(long start);

  long startGetInitialImage();

  void endGetInitialImage(long start);

  void endNoGIIDone(long start);

  void incGetInitialImageKeysReceived();

  long startIndexUpdate();

  void endIndexUpdate(long start);

  void incRegions(int inc);

  void incPartitionedRegions(int inc);

  void incDestroys();

  void incCreates();

  void incInvalidates();

  void incTombstoneCount(int amount);

  void incTombstoneGCCount();

  void incClearTimeouts();

  void incConflatedEventsCount();

  void endGet(long start, boolean miss);

  void endGetForClient(long start, boolean miss);

  long endPut(long start, boolean isUpdate);

  void endPutAll(long start);

  void endQueryExecution(long executionTime);

  void endQueryResultsHashCollisionProbe(long start);

  void incQueryResultsHashCollisions();

  void incTxConflictCheckTime(long delta);

  void txSuccess(long opTime, long txLifeTime, int txChanges);

  void txFailure(long opTime, long txLifeTime, int txChanges);

  void txRollback(long opTime, long txLifeTime, int txChanges);

  void incEventQueueSize(int items);

  void incEventQueueThrottleCount(int items);

  void incEventQueueThrottleTime(long nanos);

  void incEventThreads(int items);

  void incEntryCount(int delta);

  void incRetries();

  void incDiskTasksWaiting();

  void decDiskTasksWaiting();

  void decDiskTasksWaiting(int count);

  void incEvictorJobsStarted();

  void incEvictorJobsCompleted();

  void incEvictorQueueSize(int delta);

  void incEvictWorkTime(long delta);

  void incClearCount();

  void incPRQueryRetries();

  void incMetaDataRefreshCount();

  void endImport(long entryCount, long start);

  void endExport(long entryCount, long start);

  long startCompression();

  void endCompression(long startTime, long startSize, long endSize);

  long startDecompression();

  void endDecompression(long startTime);
}
