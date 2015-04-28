/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: AckMcastReceiverWindow.java,v 1.6 2004/09/23 16:29:53 belaban Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Address;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;




/**
 * Keeps track of messages received from various senders. Acks each message received and checks whether
 * it was already delivered. If yes, the message is discarded, otherwise it is delivered (passed up).
 * The messages contain sequence numbers of old messages to be deleted, those are removed from the
 * message table.
 * 
 * @author Bela Ban June 17 1999
 */
public class AckMcastReceiverWindow  {
    final Hashtable msgs=new Hashtable();  // sender -- Vector (of seqnos)

    protected static final GemFireTracer log=GemFireTracer.getLog(AckMcastReceiverWindow.class);



    /**
       Records the sender/seqno pair in the message table
       @param sender The sender of the message
       @param seqno The sequence number associated with the message
       @return boolean If false, message is already present. Otherwise true.
     */
    public boolean add(Object sender, long seqno) {
	Vector seqnos=(Vector)msgs.get(sender);
	Long   val=Long.valueOf(seqno);

	if(seqnos == null) {
	    seqnos=new Vector();
	    seqnos.addElement(val);
	    msgs.put(sender, seqnos);
	    return true;
	}

	if(seqnos.contains(val))
	    return false;

	seqnos.addElement(val);
	return true;	
    }




    public void remove(Object sender, Vector seqnos) {
	Vector v=(Vector)msgs.get(sender);
	Long   seqno;

	if(v != null && seqnos != null) {
	    for(int i=0; i < seqnos.size(); i++) {
		seqno=(Long)seqnos.elementAt(i);
		v.removeElement(seqno);
	    }
	}
    }



    public long size() {
	long ret=0;

	for(Enumeration e=msgs.elements(); e.hasMoreElements();) {
	    ret+=((Vector)e.nextElement()).size();
	}

	return ret;
    }


    public void reset() {
	removeAll();
    }
    
    public void removeAll() {msgs.clear();}


    public void suspect(Object sender) {

	    if(log.isInfoEnabled()) log.info(ExternalStrings.AckMcastReceiverWindow_SUSPECT_IS__0, sender);
	msgs.remove(sender);
    }



    @Override // GemStoneAddition
    public String toString() {
	StringBuffer ret=new StringBuffer();
	Object       sender;
	
	for(Enumeration e=msgs.keys(); e.hasMoreElements();) {
	    sender=e.nextElement();
	    ret.append(sender + " --> " + msgs.get(sender) + '\n');
	}
	return ret.toString();
    }






//    public static void main(String[] args) {
//	AckMcastReceiverWindow win=new AckMcastReceiverWindow();
//	Address sender1=new IpAddress("janet", 1111);
//	Address sender2=new IpAddress("janet", 4444);
//	Address sender3=new IpAddress("janet", 6767);
//	Address sender4=new IpAddress("janet", 3333);
//
//	win.add(sender1, 1);
//	win.add(sender1, 2);
//
//	win.add(sender3, 2);
//	win.add(sender2, 2);
//	win.add(sender4, 2);
//	win.add(sender1, 3);
//	win.add(sender1, 2);
//
//
//	System.out.println(win);
//
//	win.suspect(sender1);
//	System.out.println(win);
//
//	win.add(sender1, 1);
//	win.add(sender1, 2);
//	win.add(sender1, 3);
//	win.add(sender1, 4);
//	win.add(sender1, 5);
//	win.add(sender1, 6);
//	win.add(sender1, 7);
//	win.add(sender1, 8);
//
//	System.out.println(win);
//
//
//	Vector seqnos=new Vector();
//
//	seqnos.addElement(Long.valueOf(4));
//	seqnos.addElement(Long.valueOf(6));
//	seqnos.addElement(Long.valueOf(8));
//
//	win.remove(sender2, seqnos);
//	System.out.println(win);
//
//	win.remove(sender1, seqnos);
//	System.out.println(win);
//
//
//    }

}
