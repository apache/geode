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

import java.util.Collections;
import java.util.Set;

import org.apache.geode.distributed.DistributedMember;

public class MemoryThresholdInfo {
  private final boolean memoryThresholdReached;
  private final Set<DistributedMember> membersThatReachedThreshold;
  private static final MemoryThresholdInfo NOT_REACHED = new MemoryThresholdInfo(false,
      Collections.EMPTY_SET);

  MemoryThresholdInfo(boolean memoryThresholdReached,
      Set<DistributedMember> membersThatReachedThreshold) {
    this.memoryThresholdReached = memoryThresholdReached;
    this.membersThatReachedThreshold = membersThatReachedThreshold;
  }

  public Set<DistributedMember> getMembersThatReachedThreshold() {
    return membersThatReachedThreshold;
  }

  public boolean isMemoryThresholdReached() {
    return memoryThresholdReached;
  }

  static MemoryThresholdInfo getNotReached() {
    return NOT_REACHED;
  }

  @Override
  public String toString() {
    return "MemoryThresholdInfo{" +
        "memoryThresholdReached=" + memoryThresholdReached +
        ", membersThatReachedThreshold=" + membersThatReachedThreshold +
        '}';
  }
}
