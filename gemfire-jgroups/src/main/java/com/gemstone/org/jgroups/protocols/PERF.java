/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: PERF.java,v 1.10 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.stack.ProtocolObserver;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.util.Properties;
import java.util.Vector;


/**
 * Measures time taken by each protocol to process a message. PERF has to be the top protocol in a stack. It
 * adds a special header to the message which travels with the message. Upon initialization,
 * PERF creates a ProtocolObserver for each protocol layer in the stack. That observer notes the time taken for a
 * message to travel through the layer it observes, and records the start time, end time and total time
 * <em>in the message itself</em> (in the header created by PERF). When a message is received by PERF, the statistics
 * for the message are appended to a file (or displayed in a GUI (not yet done)).<p>
 * <em>Because of different wall times between different machines, PERF should only be used for members
 * in the same JVM. For members in different processes, physical clocks have to be synchronized: if they are
 * only a few milliseconds aprt, the values will be incorrect.</em>
 * For example, to write all performance data to a file, do the following:
 * <ol>
 * <li>Set tracing to on, e.g. <code>trace=true</code> in jgroups.properties
 * <li>Add a trace statement for PERF.up(): <code>trace0=PERF.up DEBUG /tmp/trace.perf</code>. This will write
 * all trace output to the given file.
 * </ol>
 *
 * @author Bela Ban Oct 2001
 * @version $Revision: 1.10 $
 */
public class PERF extends Protocol  {
    boolean             details=false;
    final Vector        members=new Vector();
    Vector              protocols=null;
    static final String name="PERF";


    /**
     * All protocol names have to be unique !
     */
    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }


    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        super.setProperties(props);
        String str;

        str=props.getProperty("details");
        if(str != null) {
            details=Boolean.valueOf(str).booleanValue();
            props.remove("details");
        }
        if(props.size() > 0) {
            log.error(ExternalStrings.PERF_PERFSETPROPERTIES_THESE_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }


    @Override // GemStoneAddition  
    public void start() throws Exception {
        protocols=stack != null ? stack.getProtocols() : null;
        setupObservers();
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Message msg;
        PerfHeader hdr;

        switch(evt.getType()) {

        case Event.MSG:
            msg=(Message)evt.getArg();
            hdr=removePerfHeader(msg);
            if(hdr != null) {
                hdr.setEndTime();
                hdr.setDone(name, PerfHeader.UP); // we do this here because header is removed, so PassUp won't find it
                if(trace)
                    log.trace(hdr.printContents(details, protocols) + "\n---------------------------------\n");
            }
            break;
        }

        passUp(evt);            // Pass up to the layer above us
    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        Message msg;

        switch(evt.getType()) {

        case Event.TMP_VIEW:
        case Event.VIEW_CHANGE:
            Vector new_members=((View)evt.getArg()).getMembers();
            synchronized(members) {
                members.removeAllElements();
                members.addAll(new_members);
            }
            passDown(evt);
            break;

        case Event.MSG:
            msg=(Message)evt.getArg();
            initializeMessage(msg); // Add a PerfHeader to the message
            break;
        }

        passDown(evt);          // Pass on to the layer below us
    }


    /* ----------------------------------- Private Methods -------------------------------------- */

    /**
     * Create PerfObservers for all protocols save PERF
     */
    void setupObservers() {
        Protocol p=null;
        String pname;
        PerfObserver po=null;

        for(int i=0; i < protocols.size(); i++) {
            p=(Protocol)protocols.elementAt(i);
            pname=p.getName();
            if(pname != null) {
                po=new PerfObserver(pname);
                p.setObserver(po);
            }
        }
    }


    void initializeMessage(Message msg) {
        PerfHeader hdr=new PerfHeader(msg.getSrc(), msg.getDest());
        Protocol p;
        String pname=null;

        if(protocols == null) {
            log.error(ExternalStrings.PERF_PERFINITIALIZEMESSAGE_PROTOCOLS_VARIABLE_IS_NULL);
            return;
        }

        for(int i=0; i < protocols.size(); i++) {
            p=(Protocol)protocols.elementAt(i);
            pname=p.getName();
            if(pname != null) {
                hdr.addEntry(pname);
            }
        }
        hdr.setReceived(name, PerfHeader.DOWN); // we do this here because down() didn't yet find a PerfHeader
        msg.putHeader(name, hdr);
    }


    /**
     * Removes if present. Otherwise returns null
     */
    PerfHeader removePerfHeader(Message m) {
        Object hdr=null;

        if(m == null || (hdr=m.removeHeader(name)) == null)
            return null;
        return (PerfHeader)hdr;
    }


//      Vector getProtocols() {
//  	Vector   ret=(Vector)stack.getProtocols().clone();
//  	Protocol p;
	
//  	for(int i=0; i < ret.size(); i++) {
//  	    p=(Protocol)ret.elementAt(i);
//  	    if(p.getName() != null && p.getName().equals(getName())) {
//  		ret.removeElement(p);
//  		break;
//  	    }
//  	}
//  	return ret;
//      }

    /* -------------------------------- End of Private Methods ---------------------------------- */
}


/**
 * Observes a protocol and adds its timings to the PerfHeader attached to each protocol.
 */
class PerfObserver implements ProtocolObserver {
    final String prot_name;
    boolean bottom=false;

    PerfObserver(String prot_name) {
        this.prot_name=prot_name;
    }


    public void setProtocol(Protocol prot) {
        if(prot != null && prot.getDownProtocol() == null)
            bottom=true;
    }


    public boolean up(Event evt, int num_evts) {
        PerfHeader hdr;
        if(evt.getType() == Event.MSG) {
            hdr=getPerfHeader((Message)evt.getArg());
            if(hdr != null) {
                hdr.setReceived(prot_name, PerfHeader.UP);
                if(bottom)
                    hdr.setNetworkReceived();
            }
        }
        return true;
    }


    public boolean passUp(Event evt) {
        PerfHeader hdr;
        if(evt.getType() == Event.MSG) {
            hdr=getPerfHeader((Message)evt.getArg());
            if(hdr != null) {
                hdr.setDone(prot_name, PerfHeader.UP);
            }
        }
        return true;
    }


    public boolean down(Event evt, int num_evts) {
        PerfHeader hdr;
        if(evt.getType() == Event.MSG) {
            hdr=getPerfHeader((Message)evt.getArg());
            if(hdr != null) {
                hdr.setReceived(prot_name, PerfHeader.DOWN);
            }
        }
        return true;
    }


    public boolean passDown(Event evt) {
        PerfHeader hdr;
        if(evt.getType() == Event.MSG) {
            hdr=getPerfHeader((Message)evt.getArg());
            if(hdr != null) {
                hdr.setDone(prot_name, PerfHeader.DOWN);
                if(bottom)
                    hdr.setNetworkSent();
            }
        }
        return true;
    }


    PerfHeader getPerfHeader(Message m) {
        Object hdr=null;

        if(m == null || (hdr=m.getHeader(PERF.name)) == null)
            return null;
        return (PerfHeader)hdr;
    }


}
