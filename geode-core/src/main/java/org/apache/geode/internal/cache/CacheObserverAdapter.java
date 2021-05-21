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

import java.nio.ByteBuffer;

import org.apache.geode.cache.RegionEvent;

/**
 * This class provides 'do-nothing' implementations of all of the methods of interface
 * CacheObserver. See the documentation for class CacheObserverHolder for details. Also the callback
 * is issued only if the boolean ISSUE_CALLBACKS_TO_CACHE_OBSERVER present in
 * org.apache.geode.internal.cache.LocalRegion is made true
 *
 */
public class CacheObserverAdapter implements CacheObserver {

  @Override
  public void afterRegionClear(RegionEvent<?, ?> event) {}

  @Override
  public void beforeDiskClear() {}

  @Override
  public void goingToFlush() {}

  @Override
  public void afterWritingBytes() {}

  @Override
  public void beforeGoingToCompact() {}

  @Override
  public void afterHavingCompacted() {}

  @Override
  public void afterConflation(ByteBuffer origBB, ByteBuffer conflatedBB) {}

  @Override
  public void afterSettingOplogOffSet(long offset) {}

  @Override
  public void beforeSwitchingOplog() {}

  @Override
  public void afterSwitchingOplog() {}

  @Override
  public void afterKrfCreated() {}

  @Override
  public void beforeStoppingCompactor() {}

  @Override
  public void afterStoppingCompactor() {}

  @Override
  public void afterSignallingCompactor() {}

  @Override
  public void afterMarkingGIICompleted() {}

  @Override
  public void afterMarkingGIIStarted() {}

  @Override
  public void afterSettingDiskRef() {}

  @Override
  public void beforeSettingDiskRef() {}

  @Override
  public void beforeDeletingCompactedOplog(Oplog compactedOplog) {}

  @Override
  public void beforeDeletingEmptyOplog(Oplog emptyOplog) {}

  @Override
  public void beforeShutdownAll() {}
}
