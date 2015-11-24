/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.distributed.internal.membership.gms.messages;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

public class SuspectRequest {
  final InternalDistributedMember suspectMember;
  final String reason;

  public SuspectRequest(InternalDistributedMember m, String r) {
    suspectMember = m;
    reason = r;
  }

  public InternalDistributedMember getSuspectMember() {
    return suspectMember;
  }

  public String getReason() {
    return reason;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((suspectMember == null) ? 0 : suspectMember.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    SuspectRequest other = (SuspectRequest) obj;
    if (suspectMember == null) {
      if (other.suspectMember != null) {
        return false;
      }
    } else if (!suspectMember.equals(other.suspectMember)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "SuspectRequest [member=" + suspectMember + ", reason=" + reason + "]";
  }
}
