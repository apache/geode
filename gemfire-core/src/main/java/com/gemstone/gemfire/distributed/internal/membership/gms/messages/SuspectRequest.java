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