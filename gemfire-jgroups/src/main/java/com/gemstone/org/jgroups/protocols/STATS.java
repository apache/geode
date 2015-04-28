/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
package com.gemstone.org.jgroups.protocols;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.util.*;

/**
 * Provides various stats
 * @author Bela Ban
 * @version $Id: STATS.java,v 1.2 2005/06/07 10:17:27 belaban Exp $
 */
public class STATS extends Protocol  {
    long sent_msgs, sent_bytes, sent_ucasts, sent_mcasts, received_ucasts, received_mcasts;
    long received_msgs, received_bytes, sent_ucast_bytes, sent_mcast_bytes, received_ucast_bytes, received_mcast_bytes;

    /** HashMap key=Address, value=Entry, maintains stats per target destination */
    HashMap sent=new HashMap();

    /** HashMap key=Address, value=Entry, maintains stats per receiver */
    HashMap received=new HashMap();

    static/*GemStoneAddition*/ final short UP=1;
    static/*GemStoneAddition*/ final short DOWN=2;


    @Override // GemStoneAddition  
    public String getName() {
        return "STATS";
    }

    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        super.setProperties(props);
        down_thread=false; // never use a down thread
        up_thread=false;   // never use an up thread

        if(props.size() > 0) {
            log.error(ExternalStrings.STATS_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);
            return false;
        }
        return true;
    }

    @Override // GemStoneAddition  
    public void resetStats() {
        sent_msgs=sent_bytes=sent_ucasts=sent_mcasts=received_ucasts=received_mcasts=0;
        received_msgs=received_bytes=sent_ucast_bytes=sent_mcast_bytes=received_ucast_bytes=received_mcast_bytes=0;
        sent.clear();
        received.clear();
    }


    public long getSentMessages() {return sent_msgs;}
    public long getSentBytes() {return sent_bytes;}
    public long getSentUnicastMessages() {return sent_ucasts;}
    public long getSentUnicastBytes() {return sent_ucast_bytes;}
    public long getSentMcastMessages() {return sent_mcasts;}
    public long getSentMcastBytes() {return sent_mcast_bytes;}

    public long getReceivedMessages() {return received_msgs;}
    public long getReceivedBytes() {return received_bytes;}
    public long getReceivedUnicastMessages() {return received_ucasts;}
    public long getReceivedUnicastBytes() {return received_ucast_bytes;}
    public long getReceivedMcastMessages() {return received_mcasts;}
    public long getReceivedMcastBytes() {return received_mcast_bytes;}


    @Override // GemStoneAddition  
    public void up(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            updateStats(msg, UP);
        }
        else if(evt.getType() == Event.VIEW_CHANGE) {
            handleViewChange((View)evt.getArg());
        }
        passUp(evt);
    }



    @Override // GemStoneAddition  
    public void down(Event evt) {
        if(evt.getType() == Event.MSG) {
            Message msg=(Message)evt.getArg();
            updateStats(msg, DOWN);
        }
        else if(evt.getType() == Event.VIEW_CHANGE) {
            handleViewChange((View)evt.getArg());
        }
        passDown(evt);
    }


    @Override // GemStoneAddition  
    public String printStats() {
        Map.Entry entry;
        Object key, val;
        StringBuffer sb=new StringBuffer();
        sb.append("sent:\n");
        for(Iterator it=sent.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            if(key == null) key="<mcast dest>";
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }
        sb.append("\nreceived:\n");
        for(Iterator it=received.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            key=entry.getKey();
            val=entry.getValue();
            sb.append(key).append(": ").append(val).append("\n");
        }

        return sb.toString();
    }

    private void handleViewChange(View view) {
        Vector members=view.getMembers();
        Set tmp=new LinkedHashSet(members);
        tmp.add(null); // for null destination (= mcast)
        sent.keySet().retainAll(tmp);
        received.keySet().retainAll(tmp);
    }

    private void updateStats(Message msg, short direction) {
        int     length;
        HashMap map;
        boolean mcast;
        Address dest, src;

        if(msg == null) return;
        length=msg.getLength();
        dest=msg.getDest();
        src=msg.getSrc();
        mcast=dest == null || dest.isMulticastAddress();

        if(direction == UP) { // received
            received_msgs++;
            received_bytes+=length;
            if(mcast) {
                received_mcasts++;
                received_mcast_bytes+=length;
            }
            else {
                received_ucasts++;
                received_ucast_bytes+=length;
            }
        }
        else {                // sent
            sent_msgs++;
            sent_bytes+=length;
            if(mcast) {
                sent_mcasts++;
                sent_mcast_bytes+=length;
            }
            else {
                sent_ucasts++;
                sent_ucast_bytes+=length;
            }
        }

        Address key=direction == UP? src : dest;
        map=direction == UP? received : sent;
        Entry entry=(Entry)map.get(key);
        if(entry == null) {
            entry=new Entry();
            map.put(key, entry);
        }
        entry.msgs++;
        entry.bytes+=length;
        if(mcast) {
            entry.mcasts++;
            entry.mcast_bytes+=length;
        }
        else {
            entry.ucasts++;
            entry.ucast_bytes+=length;
        }
    }




    static class Entry  {
        long msgs, bytes, ucasts, mcasts, ucast_bytes, mcast_bytes;

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer();
            sb.append(msgs).append(" (").append(bytes).append(" bytes)");
            sb.append(": ").append(ucasts).append(" ucasts (").append(ucast_bytes).append(" bytes), ");
            sb.append(mcasts).append(" mcasts (").append(mcast_bytes).append(" bytes)");
            return sb.toString();
        }
    }



}
