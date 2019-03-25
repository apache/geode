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

import org.apache.geode.cache.Operation;
import org.apache.geode.distributed.DistributedMember;

public class DefaultEntryEventFactory implements EntryEventFactory {
  @Override
  public EntryEventImpl create(InternalRegion region, Operation op, Object key, Object newValue,
      Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember) {
    return EntryEventImpl.create(region, op, key, newValue, callbackArgument, originRemote,
        distributedMember);
  }

  @Override
  public EntryEventImpl create(InternalRegion region, Operation op, Object key, Object newValue,
      Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, boolean generateCallbacks) {
    return EntryEventImpl.create(region, op, key, newValue, callbackArgument, originRemote,
        distributedMember, generateCallbacks);
  }

  @Override
  public EntryEventImpl create(InternalRegion region, Operation op, Object key,
      boolean originRemote, DistributedMember distributedMember,
      boolean generateCallbacks, boolean fromRILocalDestroy) {
    return EntryEventImpl.create(region, op, key, originRemote, distributedMember,
        generateCallbacks, fromRILocalDestroy);
  }

  @Override
  public EntryEventImpl create(InternalRegion region, Operation op, Object key, Object newValue,
      Object callbackArgument, boolean originRemote,
      DistributedMember distributedMember, boolean generateCallbacks,
      EventID eventID) {
    return EntryEventImpl.create(region, op, key, newValue, callbackArgument, originRemote,
        distributedMember, generateCallbacks, eventID);
  }

  @Override
  public EntryEventImpl createPutAllEvent(DistributedPutAllOperation putAllOp,
      InternalRegion region, Operation entryOp,
      Object entryKey, Object entryNewValue) {
    return EntryEventImpl.createPutAllEvent(putAllOp, region, entryOp, entryKey, entryNewValue);
  }

  @Override
  public EntryEventImpl createRemoveAllEvent(DistributedRemoveAllOperation op,
      InternalRegion region, Object entryKey) {
    return EntryEventImpl.createRemoveAllEvent(op, region, entryKey);
  }
}
