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
package org.apache.geode.internal.cache.partitioned.rebalance.model;

public enum RefusalReason {
  NONE,
  ALREADY_HOSTING,
  UNITIALIZED_MEMBER,
  SAME_ZONE,
  LAST_MEMBER_IN_ZONE,
  LOCAL_MAX_MEMORY_FULL,
  CRITICAL_HEAP;

  public boolean willAccept() {
    return this == NONE;
  }

  public String formatMessage(Member target, Bucket bucket) {
    switch (this) {
      case NONE:
        return "No reason, the move should be allowed.";
      case ALREADY_HOSTING:
        return "Target member " + target.getMemberId() + " is already hosting bucket "
            + bucket.getId();
      case UNITIALIZED_MEMBER:
        return "Target member " + target.getMemberId() + " is not fully initialized";
      case SAME_ZONE:
        return "Target member " + target.getMemberId()
            + " is in the same redundancy zone as other members hosting bucket " + bucket.getId()
            + ": " + bucket.getMembersHosting();
      case LOCAL_MAX_MEMORY_FULL:
        return "Target member " + target.getMemberId()
            + " does not have space within it's local max memory for bucket " + bucket.getId()
            + ". Bucket Size " + bucket.getBytes() + " local max memory: "
            + target.getConfiguredMaxMemory() + " remaining: " + target.getSize();
      case CRITICAL_HEAP:
        return "Target member " + target.getMemberId()
            + " has reached its critical heap percentage, and cannot accept more data";
      case LAST_MEMBER_IN_ZONE:
        return "Target member " + target.getMemberId()
            + " is the last member of redundancy zone for the bucket "
            + bucket.getId()
            + ": " + bucket.getMembersHosting();
      default:
        return this.toString();
    }
  }
}
