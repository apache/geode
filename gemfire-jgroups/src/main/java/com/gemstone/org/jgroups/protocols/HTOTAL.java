/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: HTOTAL.java,v 1.4 2005/09/01 11:41:00 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.*;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.Util;

import java.io.*;
import java.util.Properties;
import java.util.Vector;


/**
 * Implementation of UTO-TCP as designed by EPFL. Implements chaining algorithm: each sender sends the message
 * to a coordinator who then forwards it to its neighbor on the right, who then forwards it to its neighbor to the right
 * etc.
 * @author Bela Ban
 * @version $Id: HTOTAL.java,v 1.4 2005/09/01 11:41:00 belaban Exp $
 */
public class HTOTAL extends Protocol  {
    Address coord=null;
    Address neighbor=null; // to whom do we forward the message (member to the right, or null if we're at the tail)
    Address local_addr=null;
    Vector  mbrs=new Vector();
//    boolean is_coord=false; GemStoneAddition
    private boolean use_multipoint_forwarding=false;




    public HTOTAL() {
    }

    @Override // GemStoneAddition  
    public final String getName() {
        return "HTOTAL";
    }

    @Override // GemStoneAddition  
     public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("use_multipoint_forwarding");
        if(str != null) {
            use_multipoint_forwarding=Boolean.valueOf(str).booleanValue();
            props.remove("use_multipoint_forwarding");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.HTOTAL_TCPSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }

    @Override // GemStoneAddition  
    public void down(Event evt) {
        switch(evt.getType()) {
        case Event.VIEW_CHANGE:
            determineCoordinatorAndNextMember((View)evt.getArg());
            break;
        case Event.MSG:
            Message msg=(Message)evt.getArg();
            Address dest=msg.getDest();
            if(dest == null || dest.isMulticastAddress()) { // only process multipoint messages
                if(coord == null)
                    log.error(ExternalStrings.HTOTAL_COORDINATOR_IS_NULL_CANNOT_SEND_MESSAGE_TO_COORDINATOR);
                else {
                    msg.setSrc(local_addr);
                    forwardTo(coord, msg);
                }
                return; // handled here, don't pass down by default
            }
            break;
        }
        passDown(evt);
    }

    @Override // GemStoneAddition  
    public void up(Event evt) {
        switch(evt.getType()) {
        case Event.SET_LOCAL_ADDRESS:
            local_addr=(Address)evt.getArg();
            break;
        case Event.VIEW_CHANGE:
            determineCoordinatorAndNextMember((View)evt.getArg());
            break;
        case Event.MSG:
            Message msg=(Message)evt.getArg();
            HTotalHeader hdr=(HTotalHeader)msg.getHeader(getName());

            if(hdr == null)
                break;  // probably a unicast message, just pass it up

            Message copy=msg.copy(false); // do not copy the buffer
            if(use_multipoint_forwarding) {
                copy.setDest(null);
                passDown(new Event(Event.MSG, copy));
            }
            else {
                if(neighbor != null) {
                    forwardTo(neighbor, copy);
                }
            }

            msg.setDest(hdr.dest); // set destination to be the original destination
            msg.setSrc(hdr.src);   // set sender to be the original sender (important for retransmission requests)

            passUp(evt); // <-- we modify msg directly inside evt
            return;
        }
        passUp(evt);
    }

    private void forwardTo(Address destination, Message msg) {
        HTotalHeader hdr=(HTotalHeader)msg.getHeader(getName());

        if(hdr == null) {
            hdr=new HTotalHeader(msg.getDest(), msg.getSrc());
            msg.putHeader(getName(), hdr);
        }
        msg.setDest(destination);
        if(trace)
            log.trace("forwarding message to " + destination + ", hdr=" + hdr);
        passDown(new Event(Event.MSG, msg));
    }


    private void determineCoordinatorAndNextMember(View v) {
        Object tmp;
        Address retval=null;

        mbrs.clear();
        mbrs.addAll(v.getMembers());

        coord=(Address)(/* mbrs != null && GemStoneAddition (cannot be null) */ mbrs.size() > 0? mbrs.firstElement() : null);
//        is_coord=coord != null && local_addr != null && coord.equals(local_addr); GemStoneAddition

        if(/* mbrs == null || GemStoneAddition (cannot be null) */ mbrs.size() < 2 || local_addr == null)
            neighbor=null;
        else {
            for(int i=0; i < mbrs.size(); i++) {
                tmp=mbrs.elementAt(i);
                if(local_addr.equals(tmp)) {
                    if(i + 1 >= mbrs.size()) {
//                        retval=null; // we don't wrap, last member is null GemStoneAddition  (redundant assignment)
                    }
                    else
                        retval=(Address)mbrs.elementAt(i + 1);
                    break;
                }
            }
        }
        neighbor=retval;
        if(trace)
            log.trace("coord=" + coord + ", neighbor=" + neighbor);
    }


    public static class HTotalHeader extends Header implements Streamable {
        Address dest, src;

        public HTotalHeader() {
        }

        public HTotalHeader(Address dest, Address src) {
            this.dest=dest;
            this.src=src;
        }

        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(dest);
            out.writeObject(src);
        }

        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            dest=(Address)in.readObject();
            src=(Address)in.readObject();
        }

        public void writeTo(DataOutputStream out) throws IOException {
            Util.writeAddress(dest, out);
            Util.writeAddress(src, out);
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            dest=Util.readAddress(in);
            src=Util.readAddress(in);
        }

        @Override // GemStoneAddition  
        public String toString() {
            return "dest=" + dest + ", src=" + src;
        }
    }

}
