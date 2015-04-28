/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: RspList.java,v 1.5 2005/07/25 11:49:27 belaban Exp $

package com.gemstone.org.jgroups.util;


import com.gemstone.org.jgroups.Address;

import java.util.Vector;
import java.util.Collection;


/**
 * Contains responses from all members. Marks faulty members.
 * A RspList is a response list used in peer-to-peer protocols.
 */
public class RspList {
    final Vector rsps=new Vector();

    public RspList() {
        
    }

    public RspList(Collection responses) {
        this.rsps.addAll(responses);
    }


    public void reset() {
        rsps.removeAllElements();
    }



    public void addRsp(Address sender, Object retval) {
        Rsp rsp=find(sender);

        if(rsp != null) {
            rsp.sender=sender;
            rsp.retval=retval;
            rsp.received=true;
            rsp.suspected=false;
            return;
        }
        rsps.addElement(new Rsp(sender, retval));
    }


    public void addNotReceived(Address sender) {
        Rsp rsp=find(sender);

        if(rsp == null)
            rsps.addElement(new Rsp(sender));
    }


    public void addSuspect(Address sender) {
        Rsp rsp=find(sender);

        if(rsp != null) {
            rsp.sender=sender;
            rsp.retval=null;
            rsp.received=false;
            rsp.suspected=true;
            return;
        }
        rsps.addElement(new Rsp(sender, true));
    }


    public boolean isReceived(Address sender) {
        Rsp rsp=find(sender);

        if(rsp == null) return false;
        return rsp.received;
    }


    public int numSuspectedMembers() {
        int num=0;
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasSuspected())
                num++;
        }
        return num;
    }


    public Object getFirst() {
        return rsps.size() > 0 ? ((Rsp)rsps.elementAt(0)).getValue() : null;
    }


    /**
     * Returns the results from non-suspected members that are not null.
     */
    public Vector getResults() {
        Vector ret=new Vector();
        Rsp rsp;
        Object val;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasReceived() && (val=rsp.getValue()) != null)
                ret.addElement(val);
        }
        return ret;
    }


    public Vector getSuspectedMembers() {
        Vector retval=new Vector();
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.wasSuspected())
                retval.addElement(rsp.getSender());
        }
        return retval;
    }


    public boolean isSuspected(Address sender) {
        Rsp rsp=find(sender);

        if(rsp == null) return false;
        return rsp.suspected;
    }


    public Object get(Address sender) {
        Rsp rsp=find(sender);

        if(rsp == null) return null;
        return rsp.retval;
    }


    public int size() {
        return rsps.size();
    }

    public Object elementAt(int i) throws ArrayIndexOutOfBoundsException {
        return rsps.elementAt(i);
    }


    @Override // GemStoneAddition
    public String toString() {
        StringBuffer ret=new StringBuffer();
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            ret.append("[" + rsp + "]\n");
        }
        return ret.toString();
    }


    boolean contains(Address sender) {
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);

            if(rsp.sender != null && sender != null && rsp.sender.equals(sender))
                return true;
        }
        return false;
    }


    Rsp find(Address sender) {
        Rsp rsp;

        for(int i=0; i < rsps.size(); i++) {
            rsp=(Rsp)rsps.elementAt(i);
            if(rsp.sender != null && sender != null && rsp.sender.equals(sender))
                return rsp;
        }
        return null;
    }


}
