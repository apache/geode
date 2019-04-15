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
package org.apache.geode.internal.cache.event;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.InternalCacheEvent;
import org.apache.geode.internal.cache.ha.ThreadIdentifier;
import org.apache.geode.internal.cache.versions.VersionTag;

public class NonDistributedEventTracker implements EventTracker {
  @Immutable
  private static final NonDistributedEventTracker INSTANCE = new NonDistributedEventTracker();

  static final String NAME = "The NonDistributedEventTracker";

  public static NonDistributedEventTracker getInstance() {
    return INSTANCE;
  }

  private NonDistributedEventTracker() {
    // private no arg constructor to enforce singleton pattern
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {

  }

  @Override
  public void clear() {
    // nothing to clear
  }

  @Override
  public Map<ThreadIdentifier, EventSequenceNumberHolder> getState() {
    return null;
  }

  @Override
  public void recordState(InternalDistributedMember provider, Map state) {

  }

  @Override
  public void recordEvent(InternalCacheEvent event) {

  }

  @Override
  public boolean hasSeenEvent(InternalCacheEvent event) {
    return false;
  }

  @Override
  public void waitOnInitialization() throws InterruptedException {

  }

  @Override
  public VersionTag findVersionTagForSequence(EventID eventId) {
    return null;
  }

  @Override
  public VersionTag findVersionTagForBulkOp(EventID eventId) {
    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean hasSeenEvent(EventID eventID) {
    return false;
  }

  @Override
  public boolean hasSeenEvent(EventID eventID, InternalCacheEvent tagHolder) {
    return false;
  }

  @Override
  public void recordBulkOpStart(EventID eventID, ThreadIdentifier membershipID) {

  }

  @Override
  public void syncBulkOp(Runnable task, EventID eventId, boolean partOfTransaction) {
    task.run();
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public void setInitialized() {

  }

  @Override
  public boolean isInitialImageProvider(DistributedMember mbr) {
    return false;
  }

  @Override
  public ConcurrentMap<ThreadIdentifier, BulkOperationHolder> getRecordedBulkOpVersionTags() {
    return null;
  }

  @Override
  public ConcurrentMap<ThreadIdentifier, EventSequenceNumberHolder> getRecordedEvents() {
    return null;
  }

}
