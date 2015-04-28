/**
 */
package com.gemstone.org.jgroups;

/**
 * GemStoneAddition. This class is used in SUSPECT_WITH_ORIGIN events to
 * hold both the suspected members and the origin of suspicion
 *
 * @author bruce
 */
public class SuspectMember
{
  /** the source of suspicion */
  public Address whoSuspected;
  
  /** suspected member */
  public Address suspectedMember;
  
  /** create a new SuspectMember */
  public SuspectMember(Address whoSuspected, Address suspectedMember) {
    this.whoSuspected = whoSuspected;
    this.suspectedMember = suspectedMember;
  }
  
  @Override // GemStoneAddition
  public String toString() {
    return "{source="+whoSuspected+"; suspect="+suspectedMember+"}";
  }
  
  @Override // GemStoneAddition
  public int hashCode() {
    return this.suspectedMember.hashCode();
  }
  
  @Override // GemStoneAddition
  public boolean equals(Object other) {
    if ( !(other instanceof SuspectMember) ) {
      return false;
    }
    return this.suspectedMember.equals(((SuspectMember)other).suspectedMember);
  }
}
