/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: SuspectEvent.java,v 1.3 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Represents a suspect event.
 * Gives access to the suspected member.
 */
public class SuspectEvent {
    final Object suspected_mbr;
    final Object who_suspected; // GemStoneAddition

    public SuspectEvent(SuspectMember suspected_mbr) {
      this.suspected_mbr=suspected_mbr.suspectedMember;
      this.who_suspected=suspected_mbr.whoSuspected;
    }

    public Object getMember() {return suspected_mbr;}
    public Object getSuspector() { return who_suspected; } // GemStoneAddition
    @Override // GemStoneAddition
    public String toString() {return "SuspectEvent";}
}
