/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: WANPING.java,v 1.10 2005/05/30 14:31:24 belaban Exp $

package com.gemstone.org.jgroups.protocols;



import java.util.Enumeration;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.List;


/**
 * Similar to TCPPING, except that the initial host list is specified as a list of logical pipe names.
 */
public class WANPING extends Discovery  {
//    int port_range=5;        // number of ports to be probed for initial membership GemStoneAddition(omitted)
    List initial_hosts=null;  // hosts to be contacted for the initial membership


    @Override // GemStoneAddition
    public String getName() {
        return "WANPING";
    }


    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;
        str=props.getProperty("port_range");           // if member cannot be contacted on base port,
        if(str != null) {                              // how many times can we increment the port
//            port_range=Integer.parseInt(str); GemStoneAddition
            props.remove("port_range");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            initial_hosts=createInitialHosts(str);
            if(log.isInfoEnabled()) log.info(ExternalStrings.WANPING_INITIAL_HOSTS__0, initial_hosts);
        }

        if(initial_hosts == null || initial_hosts.size() == 0) {
            log.error("WANPING.setProperties(): hosts to contact for initial membership " +
                               "not specified. Cannot determine coordinator !");
            return false;
        }
        return super.setProperties(props);
    }

    @Override // GemStoneAddition
    public void sendGetMembersRequest(AtomicBoolean waiter_sync) {
        Message msg, copy;
        PingHeader hdr;
        String h;

        hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
        msg=new Message(null, null, null);
        msg.putHeader(getName(), hdr);
        
        wakeWaiter(waiter_sync);

        for(Enumeration en=initial_hosts.elements(); en.hasMoreElements();) {
            h=(String)en.nextElement();
            copy=msg.copy();
            copy.setDest(new WanPipeAddress(h));
            passDown(new Event(Event.MSG, copy));
        }
    }



    /* -------------------------- Private methods ---------------------------- */

    /**
     * Input is "pipe1,pipe2". Return List of Strings
     */
    private List createInitialHosts(String l) {
        List tmp=new List();
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;

        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                tmp.add(t.trim());
            }
            catch(NumberFormatException e) {
                log.error(ExternalStrings.WANPING_WANPINGCREATEINITIALHOSTS__0, e);
            }
        }
        return tmp;
    }


}

