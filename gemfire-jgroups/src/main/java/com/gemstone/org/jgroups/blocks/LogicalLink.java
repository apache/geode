/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: LogicalLink.java,v 1.5 2005/05/30 16:14:34 belaban Exp $

package com.gemstone.org.jgroups.blocks;

import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.Vector;


/**
 * Implements a logical point-to-point link between 2 entities consisting of a number of physical links.
 * Traffic is routed over any of the physical link, according to policies. Examples are: send traffic
 * over all links, round-robin, use first link for 70% of traffic, other links for the remaining 30%.
 *
 * @author Bela Ban, June 2000
 */
public class LogicalLink implements Link.Receiver {
    Receiver receiver=null;
    final Vector links=new Vector();  // of Links
    static/*GemStoneAddition*/ final int link_to_use=0;
    GemFireTracer log=GemFireTracer.getLog(getClass());


    static/*GemStoneAddition*/ public class NoLinksAvailable extends Exception {
      private static final long serialVersionUID = -4180512062659195788L;
      @Override // GemStoneAddition
      public String toString() {
        return "LogicalLinks.NoLinksAvailable: there are no physical links available";
      }
    }

    static/*GemStoneAddition*/ public class AllLinksDown extends Exception {
      private static final long serialVersionUID = -2294651737997827005L;
      @Override // GemStoneAddition
        public String toString() {
            return "LogicalLinks.AllLinksDown: all physical links are currently down";
        }
    }


    public interface Receiver {
        void receive(byte[] buf);

        void linkDown(InetAddress local, int local_port, InetAddress remote, int remote_port);

        void linkUp(InetAddress local, int local_port, InetAddress remote, int remote_port);

        void missedHeartbeat(InetAddress local, int local_port, InetAddress remote, int remote_port, int num_hbs);

        void receivedHeartbeatAgain(InetAddress local, int local_port, InetAddress remote, int remote_port);
    }


    public LogicalLink(Receiver r) {
        receiver=r;

    }

    public LogicalLink() {

    }


    public void addLink(String local_addr, int local_port, String remote_addr, int remote_port) {
        Link new_link=new Link(local_addr, local_port, remote_addr, remote_port, this);
        if(links.contains(new_link))
            log.error(ExternalStrings.LogicalLink_LOGICALLINKADD_LINK__0__IS_ALREADY_PRESENT, new_link);
        else
            links.addElement(new_link);
    }


    public void addLink(String local_addr, int local_port, String remote_addr, int remote_port,
                        long timeout, long hb_interval) {
        Link new_link=new Link(local_addr, local_port, remote_addr, remote_port, timeout, hb_interval, this);
        if(links.contains(new_link))
            log.error(ExternalStrings.LogicalLink_LOGICALLINKADD_LINK__0__IS_ALREADY_PRESENT, new_link);
        else
            links.addElement(new_link);
    }


    public void removeAllLinks() {
        Link tmp;
        for(int i=0; i < links.size(); i++) {
            tmp=(Link)links.elementAt(i);
            tmp.stop();
        }
        links.removeAllElements();
    }


    public Vector getLinks() {
        return links;
    }


    public int numberOfLinks() {
        return links.size();
    }


    public int numberOfEstablishedLinks() {
        int n=0;

        for(int i=0; i < links.size(); i++) {
            if(((Link)links.elementAt(i)).established())
                n++;
        }
        return n;
    }


    /**
     * Start all links
     */
    public void start() {
        Link tmp;
        for(int i=0; i < links.size(); i++) {
            tmp=(Link)links.elementAt(i);
            try {
                tmp.start();
            }
            catch(Exception ex) {
                log.error(ExternalStrings.LogicalLink_LOGICALLINKSTART_COULD_NOT_CREATE_PHYSICAL_LINK_REASON__0, ex);
            }
        }
    }


    /**
     * Stop all links
     */
    public void stop() {
        Link tmp;
        for(int i=0; i < links.size(); i++) {
            tmp=(Link)links.elementAt(i);
            tmp.stop();
        }
    }


    /**
     * Send a message to the other side
     */
    public boolean send(byte[] buf) throws AllLinksDown, NoLinksAvailable {
        Link link;
        int link_used=0;

        if(buf == null || buf.length == 0) {
            log.error(ExternalStrings.LogicalLink_LOGICALLINKSEND_BUF_IS_NULL_OR_EMPTY);
            return false;
        }

        if(links.size() == 0)
            throw new NoLinksAvailable();



        // current policy (make policies configurable later !): alternate between links.
        // if failure, take first link that works
        //  	link=(Link)links.elementAt(link_to_use);
        //  	if(link.send(buf)) {
        //  	    System.out.println("Send over link #" + link_to_use + ": " + link);
        //  	    link_to_use=(link_to_use + 1) % links.size();
        //  	    return true;
        //  	}

        //  	link_used=(link_to_use + 1) % links.size();
        //  	while(link_used != link_to_use) {
        //  	    link=(Link)links.elementAt(link_used);
        //  	    if(link.send(buf)) {
        //  		System.out.println("Send over link #" + link_used + ": " + link);
        //  		link_to_use=(link_to_use + 1) % links.size();
        //  		return true;
        //  	    }
        //  	    link_used=(link_used + 1) % links.size();
        //  	}




        // take first available link. use other links only if first is down. if we have smaller and bigger
        // pipes, the bigger ones should be specified first (so we're using them first, and only when they
        // are not available we use the smaller ones)
        for(int i=0; i < links.size(); i++) {
            link=(Link)links.elementAt(i);
            if(link.established()) {
                if(link.send(buf)) {
                    System.out.println("Send over link #" + link_used + ": " + link);
                    return true;
                }
            }
        }

        throw new AllLinksDown();
    }


    public void setReceiver(Receiver r) {
        receiver=r;
    }


    /*-------- Interface Link.Receiver ---------*/

    /**
     * Receive a message from any of the physical links. That's why this and the next methods have to be
     * synchronized
     */
    public synchronized void receive(byte[] buf) {
        if(receiver != null)
            receiver.receive(buf);
    }

    /**
     * One of the physical links went down
     */
    public synchronized void linkDown(InetAddress local, int local_port, InetAddress remote, int remote_port) {
        if(receiver != null)
            receiver.linkDown(local, local_port, remote, remote_port);
    }

    /**
     * One of the physical links came up
     */
    public synchronized void linkUp(InetAddress local, int local_port, InetAddress remote, int remote_port) {
        if(receiver != null)
            receiver.linkUp(local, local_port, remote, remote_port);
    }


    /**
     * Missed one or more heartbeats. Link is not yet down, though
     */
    public synchronized void missedHeartbeat(InetAddress local, int local_port,
                                             InetAddress remote, int remote_port, int num_missed_hbs) {
        if(receiver != null)
            receiver.missedHeartbeat(local, local_port, remote, remote_port, num_missed_hbs);
    }


    /**
     * Heartbeat came back again (before link was taken down) after missing some heartbeats
     */
    public synchronized void receivedHeartbeatAgain(InetAddress local, int local_port,
                                                    InetAddress remote, int remote_port) {
        if(receiver != null)
            receiver.receivedHeartbeatAgain(local, local_port, remote, remote_port);
    }


    protected/*GemStoneAddition*/ static class MyReceiver implements LogicalLink.Receiver {

        public void receive(byte[] buf) {
            System.out.println("<-- " + new String(buf));
        }


        /**
         * All of the physical links are down --> logical link is down too
         */
        public synchronized void linkDown(InetAddress l, int lp, InetAddress r, int rp) {
            System.out.println("** linkDown(): " + r + ':' + rp);
        }

        /**
         * At least 1 physical links is up again
         */
        public synchronized void linkUp(InetAddress l, int lp, InetAddress r, int rp) {
            System.out.println("** linkUp(): " + r + ':' + rp);
        }

        public synchronized void missedHeartbeat(InetAddress l, int lp, InetAddress r, int rp, int num) {
            //System.out.println("missedHeartbeat(): " + r + ":" + rp);
        }

        public synchronized void receivedHeartbeatAgain(InetAddress l, int lp, InetAddress r, int rp) {
            //System.out.println("receivedHeartbeatAgain(): " + r + ":" + rp);
        }

    }


//    public static void main(String[] args) {
//        LogicalLink ll=new LogicalLink();
//        String local_host, remote_host;
//        int local_port, remote_port;
//        int i=0;
//
//        ll.setReceiver(new MyReceiver());
//
//        if(args.length % 4 != 0 || args.length == 0) {
//            System.err.println("\nLogicalLink <link+>\nwhere <link> is " +
//                               "<local host> <local port> <remote host> <remote port>\n");
//            return;
//        }
//
//        while(i < args.length) {
//            local_host=args[i++];
//            local_port=Integer.parseInt(args[i++]);
//            remote_host=args[i++];
//            remote_port=Integer.parseInt(args[i++]);
//            ll.addLink(local_host, local_port, remote_host, remote_port);
//        }
//
//        try {
//            ll.start();
//        }
//        catch(Exception e) {
//            System.err.println("LogicalLink.main(): " + e);
//        }
//
//        BufferedReader in=new BufferedReader(new InputStreamReader(System.in));
//        while(true) {
//            try {
//                System.out.print("> ");
//                System.out.flush();
//                String line=in.readLine();
//                ll.send(line.getBytes());
//            }
//            catch(Exception e) {
//                System.err.println(e);
//            }
//        }
//
//    }


}


