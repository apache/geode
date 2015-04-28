/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GetStateEvent.java,v 1.4 2005/07/17 11:38:05 chrislott Exp $

package com.gemstone.org.jgroups;

/**
 * Represents a GetState event.
 * Gives access to the requestor.
 */
public class GetStateEvent {
    Object requestor=null;

    public GetStateEvent(Object requestor) {this.requestor=requestor;}

    public Object getRequestor() {return requestor;}

    @Override // GemStoneAddition
    public String toString() {return "GetStateEvent[requestor=" + requestor + ']';}
}
