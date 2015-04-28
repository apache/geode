/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: TCPGOSSIP.java,v 1.16 2005/08/11 12:43:47 belaban Exp $

package com.gemstone.org.jgroups.protocols;


import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.stack.GossipClient;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * The TCPGOSSIP protocol layer retrieves the initial membership (used by the GMS when started
 * by sending event FIND_INITIAL_MBRS down the stack).
 * We do this by contacting one or more GossipServers, which must be running at well-known
 * addresses:ports. The responses should allow us to determine the coordinator whom we have to
 * contact, e.g. in case we want to join the group.  When we are a server (after having
 * received the BECOME_SERVER event), we'll respond to TCPGOSSIP requests with a TCPGOSSIP
 * response.<p> The FIND_INITIAL_MBRS event will eventually be answered with a
 * FIND_INITIAL_MBRS_OK event up the stack.
 *
 * @author Bela Ban
 */
public class TCPGOSSIP extends Discovery  {
    Vector initial_hosts=null;  // (list of IpAddresses) hosts to be contacted for the initial membership
    GossipClient gossip_client=null;  // accesses the GossipServer(s) to find initial mbrship

    // we need to refresh the registration with the GossipServer(s) periodically,
    // so that our entries are not purged from the cache
    long gossip_refresh_rate=20000;
    
    private boolean splitBrainDetectionEnabled; // GemStoneAddition
    private int gossipServerWaitTime; // GemStoneAddition

    final static Vector EMPTY_VECTOR=new Vector();
    final static String name="TCPGOSSIP";


    @Override // GemStoneAddition
    public String getName() {
        return name;
    }

    // start GemStoneAddition
    @Override // GemStoneAddition
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumTCPGOSSIP;
    }
    // end GemStone addition

    @Override // GemStoneAddition
    public boolean setProperties(Properties props) {
        String str;
        str=props.getProperty("gossip_refresh_rate");  // wait for at most n members
        if(str != null) {
            gossip_refresh_rate=Integer.parseInt(str);
            props.remove("gossip_refresh_rate");
        }

        //GemStoneAddition - split-brain detection support
        str=props.getProperty("split-brain-detection");
        if (str != null) {
          splitBrainDetectionEnabled = Boolean.valueOf(str).booleanValue();
          props.remove("split-brain-detection");
        }

        str=props.getProperty("initial_hosts");
        if(str != null) {
            props.remove("initial_hosts");
            initial_hosts=createInitialHosts(str);
        }
        
        str = props.getProperty("gossip_server_wait_time");
        if (str != null) {
          props.remove("gossip_server_wait_time");
          gossipServerWaitTime = Integer.parseInt(str);
        }

        if(initial_hosts == null || initial_hosts.size() == 0) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.TCPGOSSIP_INITIAL_HOSTS_MUST_CONTAIN_THE_ADDRESS_OF_AT_LEAST_ONE_GOSSIPSERVER);
            return false;
        }
        return super.setProperties(props);
    }



    @Override // GemStoneAddition
    public void start() throws Exception {
        super.start();
        if(gossip_client == null) {
            gossip_client=new GossipClient(initial_hosts, gossip_refresh_rate, this.stack);
            gossip_client.setTimeout((int)this.timeout);
        }
    }

    @Override // GemStoneAddition
    public void stop() {
        super.stop();
        if(gossip_client != null) {
            gossip_client.stop();
            //gossip_client=null;
        }
    }


    @Override // GemStoneAddition
    public void handleConnectOK() {
        if(group_addr == null || local_addr == null) {
            if(log.isErrorEnabled())
                log.error("[CONNECT_OK]: group_addr or local_addr is null. " +
                          "cannot register with GossipServer(s)");
        }
        else {
            gossip_client.register(group_addr, local_addr, timeout, true);  // GemStone - timeout, stack & inhibit registration
        }
    }


    private boolean ipWarningIssued; // GemStoneAddition - IP version checking

    @Override // GemStoneAddition
    public void sendGetMembersRequest(AtomicBoolean waiter_sync) { // GemStoneAddition - both parameters
        Message msg, copy;
        PingHeader hdr;
        Vector tmp_mbrs;
        Address mbr_addr;
        GossipClient client = gossip_client; // GemStoneAddition - gossip_client gets nulled when this proto is stopped

        // bug #41484 - only use coordinator advice from the gossip server once
        boolean shortcutOK = !this.stack.hasTriedJoinShortcut();

        if(group_addr == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.TCPGOSSIP_FIND_INITIAL_MBRS_GROUP_ADDR_IS_NULL_CANNOT_GET_MBRSHIP);
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, EMPTY_VECTOR));
            return;
        }
        if(trace) log.trace("fetching members from GossipServer(s)");

        // GemStoneAddition - bug 28965: don't allow startup if no gossip server
        boolean isAdminOnly = stack.gfPeerFunctions.isAdminOnlyMember();
        //do {  GemStone - see comment below
        
          if (gossip_client == null)
            return;

          long giveUpTime = System.currentTimeMillis() + (this.gossipServerWaitTime * 1000L);

          tmp_mbrs=client.getMembers(group_addr, local_addr, true, this.timeout); // GemStoneAddition - send local addr on get
          
          boolean firstWait = true;
          boolean startupStatusWaitingSet = false;
          
//          if (isAdminOnly) { // GemStoneAddition - this if-else block added
            while (gossip_client != null && client.getResponsiveServerCount() == 0 ||  tmp_mbrs == null || tmp_mbrs.size() == 0) {
              // Wait, until we can contact at least one of our
              // gossip servers and it had someone register with it
              if (!isAdminOnly  &&  System.currentTimeMillis() >= giveUpTime) {
                break;
              }
              if (firstWait) {
                StringBuilder sb = new StringBuilder(100);
                for (Object obj: this.initial_hosts) {
                  if (!firstWait) {
                    sb.append(',');
                  }
                  firstWait = false;
                  IpAddress addr = (IpAddress)obj;
                  sb.append(addr.getIpAddress().getHostName())
                    .append('[')
                    .append(addr.getPort())
                    .append(']');
                }
                // inform gfsh / ServerLauncher
                startupStatusWaitingSet = true;
                stack.gfPeerFunctions.logStartup(ExternalStrings.WAITING_FOR_LOCATOR_TO_START,sb.toString());
              }
              try {
                Thread.sleep(1000);
              } catch (InterruptedException ignore) {
                Thread.currentThread().interrupt(); // GemStoneAddition
                return; // GemStoneAddition
              }
              tmp_mbrs=client.getMembers(group_addr, local_addr, true, timeout);
              // GemStone Addition 08-04-04
              // if the VM is exiting, return so that the distributed system
              // sync can be released and the shutdown hook can do its job
              if (stack.gfPeerFunctions.shutdownHookIsAlive()) {
                throw stack.gfBasicFunctions.getGemFireConfigException("Unable to contact a Locator service before detecting that VM is exiting");
              }
            }
//          } else {
//            if (gossip_client == null) GemStoneAddition (this is never null)
//              return;
            if (client.getResponsiveServerCount() == 0) {
              RuntimeException re = stack.gfBasicFunctions.getGemFireConfigException("Unable to contact a Locator service.  Operation either timed out or Locator does not exist.  Configured list of locators is \"" + initial_hosts + "\".");
              throw re;
            }
//          }

            if (startupStatusWaitingSet) {
              stack.gfPeerFunctions.logStartup(ExternalStrings.WAITING_FOR_LOCATOR_TO_START_COMPLETED);
            }
        Set<Address> serverAddresses = client.getServerAddresses();
        
        if (client.getFloatingCoordinatorDisabled()) {
          passUp(new Event(Event.FLOATING_COORDINATOR_DISABLED, null));
        }
        
        if (client.getNetworkPartitionDetectionEnabled() != splitBrainDetectionEnabled) {
          if (!splitBrainDetectionEnabled) {
            splitBrainDetectionEnabled = true;
            passUp(new Event(Event.ENABLE_NETWORK_PARTITION_DETECTION));
          } else {
            throw stack.gfBasicFunctions.getGemFireConfigException("Locator has enable-network-partition-detection="
              + client.getNetworkPartitionDetectionEnabled()
              +" but this member has enable-network-partition-detection="
              + splitBrainDetectionEnabled);
          }
        }
        
        if (client.getNetworkPartitionDetectionEnabled()) {
          stack.gfBasicFunctions.checkDisableDNS();
        }
          

        // GemStoneAddition for bug 39220 see if we're using an incompatible
        // version of IP
        if (tmp_mbrs != null && !ipWarningIssued) {
          TP protocol = (TP)stack.findProtocol("UDP");
          if (protocol == null) protocol = (TP)stack.findProtocol("TCP");
          InetAddress bindAddress = protocol.getInetBindAddress();
          if (bindAddress != null) {
            boolean iAmIPv4 = (bindAddress instanceof Inet4Address);
            for (int i=0; i<tmp_mbrs.size(); i++) {
              IpAddress addr = (IpAddress)tmp_mbrs.get(i);
              InetAddress iaddr = addr.getIpAddress();
              if (iAmIPv4 != (iaddr instanceof Inet4Address)) {
                // incompatible addresses are being used
                log.getLogWriter().warning(
                  ExternalStrings.TCPGOSSIP_IP_VERSION_MISMATCH);
                ipWarningIssued = true;
                break;
              }
            }
          }
        }
        
        serverAddresses.remove(this.local_addr);
        this.ping_waiter.setRequiredResponses(serverAddresses);

        // GemStoneAddition - if no locators have distributed systems,
        // tell the GMS that it's okay for it to become a coordinator  
        if (client.getServerDistributedSystemCount() == 0) {
          passUp(new Event(Event.ENABLE_INITIAL_COORDINATOR, null));
        }
        
        // GemStoneAddition - shortcut the get_mbrs phase
        if (shortcutOK) {
          Address coordinator = client.getCoordinator();
          // if this is a Locator starting up and there are no other processes
          // in the system we can bypass discovery

          // disabled: this allows a locator that's starting up to ignore concurrently
          // starting locators.  bug #30341 is fixed by requiring responses from
          // all known locators during discovery, and this code messes that up
//          if (coordinator == null && Locator.hasLocators()
//              && tmp_mbrs.size() == 0
//              || (tmp_mbrs.size() == 1 && tmp_mbrs.get(0).equals(this.local_addr))) {
//            coordinator = this.local_addr;
//          }
          if (coordinator != null) {
            if (log.getLogWriter().fineEnabled()) {
              log.getLogWriter().fine("Locator returned coordinator " + coordinator +
              ", so bypassing unicast discovery processing");
            }
            ping_waiter.setCoordinator(coordinator);
            wakeWaiter(waiter_sync);
            return;
          }
        }
          
        if(tmp_mbrs == null || tmp_mbrs.size() == 0) {
            if(trace) log.trace("[FIND_INITIAL_MBRS]: gossip client found no members");
            passUp(new Event(Event.FIND_INITIAL_MBRS_OK, EMPTY_VECTOR));
            wakeWaiter(waiter_sync); // GemStoneAddition
            return;
        }
        if(trace) {
          log.trace("consolidated mbrs from GossipServer(s) are " + tmp_mbrs
              + ".  Locator distributed system count=" + client.getServerDistributedSystemCount()
              + ", and floatingCoordinationDisabled="+client.getFloatingCoordinatorDisabled());
        }

        // GemStoneAddition - forces us to not get any initial member responses & tests the
        // disable_initial_coordinator setting
        //if (true) {
        //  log.info("DEBUG: not sending GET_MBRS_REQ message to list returned by gossip server");
        //  return;
        //}

        // 1. 'Mcast' GET_MBRS_REQ message
        hdr=new PingHeader(PingHeader.GET_MBRS_REQ, null);
        msg=new Message(null, null, null);
        msg.putHeader(name, hdr);
        //GemStoneAddition - don't bundle this message or we might time out
        // before it's even sent
        msg.bundleable = false;

        wakeWaiter(waiter_sync); // GemStoneAddition
        
        // GemStoneAddition - here we send the request to newer members first
        // since they're likely to be around.
        int max_msgs = Integer.getInteger("gemfire.max_ping_requests", 40).intValue();
        int msgs_sent = 0;
        for(int i=tmp_mbrs.size()-1; i >= 0; i--) {
            mbr_addr=(Address)tmp_mbrs.elementAt(i);
            // make sure all required responders get the message
            if (!serverAddresses.contains(mbr_addr)  &&  (msgs_sent >= max_msgs)) {
              continue;
            }
            copy=msg.copy();
            copy.setDest(mbr_addr);
            if(trace) log.trace("[FIND_INITIAL_MBRS] sending PING request to " + copy.getDest());
            passDown(new Event(Event.MSG, copy));
            if (Thread.currentThread().isInterrupted()) {
              break;
            }
            msgs_sent++;
        }
          
          // GemStoneAddition - not really an addition, just a note from Bruce
          // that this used to have a wait-for-initial-members section that is
          // now gone, making the loop a bit difficult to implement
        //} while (isAdminOnly && initial_members.size() <= 0);
        
    }



    /* -------------------------- Private methods ---------------------------- */


    /**
     * Input is "daddy[8880],sindhu[8880],camille[5555]. Return list of IpAddresses
     */
    public static Vector createInitialHosts(String l) {
        Vector tmp=new Vector();
        String host;
        int port;
        IpAddress addr;
        StringTokenizer tok=new StringTokenizer(l, ",");
        String t;
        boolean isLoopback = false;
        InetAddress myAddress = null;
        
        String bindAddress = System.getProperty("gemfire.jg-bind-address");
        try {
          if (bindAddress == null) {
            isLoopback = JChannel.getGfFunctions().getLocalHost().isLoopbackAddress(); 
          } else {
            isLoopback = InetAddress.getByName(bindAddress).isLoopbackAddress();
          }
        } catch (UnknownHostException e) {
          // ignore
        }
        


        while(tok.hasMoreTokens()) {
            try {
                t=tok.nextToken();
                host=t.substring(0, t.indexOf('['));
                // GemStoneAddition - support for name:bind-addr[port] format
                int idx = host.lastIndexOf('@');
                if (idx < 0) {
                  idx = host.lastIndexOf(':');
                }
                String h = host.substring(0, idx > -1 ? idx : host.length());
                if (h.indexOf(':') >= 0) { // a single numeric ipv6 address
                  idx = host.lastIndexOf('@');
                }
                if (idx >= 0) {
                  host = host.substring(idx+1, host.length());
                }
                port=Integer.parseInt(t.substring(t.indexOf('[') + 1, t.indexOf(']')));
                addr=new IpAddress(host, port);
                if (isLoopback && !addr.getIpAddress().isLoopbackAddress()) { // GemStoneAddition
                  // TODO this should be a GemFireConfigException but that class isn't available
                  // in a static method in the jgroups project
                  throw new RuntimeException("This process is attempting to join with a loopback address ("+myAddress+") using a locator that does not have a local address ("+addr.getIpAddress()+").  On Unix this usually means that /etc/hosts is misconfigured.");
                }
                tmp.addElement(addr);
            }
            catch(NumberFormatException e) {
                //if(log.isErrorEnabled()) log.error(JGroupsStrings.TCPGOSSIP_EXEPTION_IS__0, e);
            }
        }

        return tmp;
    }
    
    @Override // GemStoneAddition
    public void destroy() { // GemStoneAddition - get rid of gossip timer
      if (gossip_client != null) {
        gossip_client.destroy();
        gossip_client = null;
      }
    }


}

