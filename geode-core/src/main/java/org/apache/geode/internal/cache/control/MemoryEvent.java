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

package org.apache.geode.internal.cache.control;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.control.InternalResourceManager.ResourceType;
import org.apache.geode.internal.cache.control.MemoryThresholds.MemoryState;

/**
 */
public class MemoryEvent implements ResourceEvent {
  private final ResourceType type;
  private final MemoryState state;
  private final MemoryState previousState;
  private final DistributedMember member;
  private final long bytesUsed;
  private final boolean isLocal;
  private final MemoryThresholds thresholds;

  public MemoryEvent(final ResourceType type, final MemoryState previousState,
      final MemoryState state, final DistributedMember member, final long bytesUsed,
      final boolean isLocal, final MemoryThresholds thresholds) {
    this.type = type;
    this.previousState = previousState;
    this.state = state;
    this.member = member;
    this.bytesUsed = bytesUsed;
    this.isLocal = isLocal;
    this.thresholds = thresholds;
  }

  @Override
  public ResourceType getType() {
    return this.type;
  }

  public MemoryState getPreviousState() {
    return this.previousState;
  }

  public MemoryState getState() {
    return this.state;
  }

  @Override
  public DistributedMember getMember() {
    return this.member;
  }

  public long getBytesUsed() {
    return this.bytesUsed;
  }

  @Override
  public boolean isLocal() {
    return this.isLocal;
  }

  public MemoryThresholds getThresholds() {
    return this.thresholds;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("MemoryEvent@").append(System.identityHashCode(this))
        .append("[Member:" + this.member).append(",type:" + this.type)
        .append(",previousState:" + this.previousState).append(",state:" + this.state)
        .append(",bytesUsed:" + this.bytesUsed).append(",isLocal:" + this.isLocal)
        .append(",thresholds:" + this.thresholds + "]").toString();
  }
}

