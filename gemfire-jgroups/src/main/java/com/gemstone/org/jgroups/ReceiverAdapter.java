/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups;

/**
 * @author Bela Ban
 * @version $Id: ReceiverAdapter.java,v 1.1 2005/11/08 10:43:38 belaban Exp $
 */
public class ReceiverAdapter implements Receiver {

    public void receive(Message msg) {
    }

    public byte[] getState() {
        return null;
    }

    public void setState(byte[] state) {
    }

    public void viewAccepted(View new_view) {
    }

    public void suspect(SuspectMember suspected_mbr) {
    }

    public void block() {
    }

    public void channelClosing(Channel c, Exception e) {} // GemStoneAddition
    
    
}
