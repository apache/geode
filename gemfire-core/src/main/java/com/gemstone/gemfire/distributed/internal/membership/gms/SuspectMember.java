/**
 */
package com.gemstone.gemfire.distributed.internal.membership.gms;

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;

/** represents a suspicion raised about a member */
public class SuspectMember
{
  /** the source of suspicion */
  public InternalDistributedMember whoSuspected;
  
  /** suspected member */
  public InternalDistributedMember suspectedMember;
  
  /** create a new SuspectMember */
  public SuspectMember(InternalDistributedMember whoSuspected, InternalDistributedMember suspectedMember) {
    this.whoSuspected = whoSuspected;
    this.suspectedMember = suspectedMember;
  }
  
  @Override
  public String toString() {
    return "{source="+whoSuspected+"; suspect="+suspectedMember+"}";
  }
  
  @Override
  public int hashCode() {
    return this.suspectedMember.hashCode();
  }
  
  @Override
  public boolean equals(Object other) {
    if ( !(other instanceof SuspectMember) ) {
      return false;
    }
    return this.suspectedMember.equals(((SuspectMember)other).suspectedMember);
  }
}
