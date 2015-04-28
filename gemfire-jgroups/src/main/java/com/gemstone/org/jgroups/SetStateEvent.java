/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: SetStateEvent.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;






/**
 * Encapsulates a state returned by <code>Channel.receive()</code>, as requested by
 * <code>Channel.getState(s)</code> previously. State could be a single state (as requested by
 * <code>Channel.getState()</code>) or a vector of states (as requested by
 * <code>Channel.getStates()</code>).
 * @author Bela Ban
 */
public class SetStateEvent {
    byte[]     state=null;         // state


    public SetStateEvent(byte[] state) {
	this.state=state;
    }

    public byte[] getArg() {return state;}

    @Override // GemStoneAddition
    public String toString() {return "SetStateEvent[state=" + 
//      state
      (state == null ? "null" : "(" + state.length + " bytes)") // GemStoneAddition
      + ']';}

}
