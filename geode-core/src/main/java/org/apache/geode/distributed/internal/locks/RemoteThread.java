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

package org.apache.geode.distributed.internal.locks;

import org.apache.geode.distributed.DistributedMember;

/**
 * Uniquely identifies a remote thread by DistributedMember and threadId sequence that is unique
 * within the DLockService instance of that member.
 */
public class RemoteThread {

  private final DistributedMember member;
  private final int threadId;

  /** Constructs new immutable instance of RemoteThread */
  RemoteThread(DistributedMember member, int threadId) {
    this.member = member;
    this.threadId = threadId;
  }

  public DistributedMember getDistributedMember() {
    return this.member;
  }

  public int getThreadId() {
    return this.threadId;
  }

  @Override
  public boolean equals(Object other) {
    if (other == this)
      return true;
    if (other == null)
      return false;
    if (!(other instanceof RemoteThread))
      return false;
    final RemoteThread that = (RemoteThread) other;

    if (this.member != that.member && !(this.member != null && this.member.equals(that.member)))
      return false;
    if (this.threadId != that.threadId)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    final int mult = 37;

    result = mult * result + (this.member == null ? 0 : this.member.hashCode());
    result = mult * result + this.threadId;

    return result;
  }

  /**
   * Returns a string representation of the object.
   * 
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("[RemoteThread@");
    sb.append(System.identityHashCode(this)).append(": ");
    sb.append("member@").append(System.identityHashCode(this.member)).append("=")
        .append(this.member);
    sb.append(", threadId=").append(this.threadId);
    sb.append("]");
    return sb.toString();
  }

}

