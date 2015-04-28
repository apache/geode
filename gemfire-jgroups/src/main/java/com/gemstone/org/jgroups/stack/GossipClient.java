/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GossipClient.java,v 1.11 2005/07/17 11:34:20 chrislott Exp $

package com.gemstone.org.jgroups.stack;


import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Channel;
import com.gemstone.org.jgroups.JChannel;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.util.ConnectionWatcher;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;

import javax.net.ssl.SSLException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.*;
//import org.apache.commons.logging.LogFactory;

// @todo Make access to multiple GossipServer concurrent (1 thread/GossipServer).
/**
 * Local stub for clients to access one (or more) GossipServers. Will use proprietary protocol
 * (using GossipData PDUs) based on TCP to connect to GossipServer.<p>
 * Requires JDK >= 1.3 due to the use of Timer.
 * 
 * @author Bela Ban Oct 4 2001
 */
public class GossipClient  {
    Timer timer=new Timer(true /* GemStoneAddition */);
    // GemStoneAddition - fix for bug #43027 occurrence from 3/29/11
    Timer connectionTimer;
    //Timer timer=new Timer();
    final Hashtable groups=new Hashtable();               // groups - Vector of Addresses
    Refresher refresher_task=new Refresher();
    final Vector gossip_servers=new Vector();          // a list of GossipServers (IpAddress)
    Set<Address> serverAddresses;  // server addresses

    public Vector getGossip_servers() {
      synchronized(this.gossip_servers) {
        return new Vector(this.gossip_servers);
      }
    }
    
    /** GemStoneAddition - get the distribution addresses for responding servers */
    public Set<Address> getServerAddresses() {
      if (this.serverAddresses == null) {
        return Collections.EMPTY_SET;
      }
      return this.serverAddresses;
    }


    boolean timer_running=false;
    long EXPIRY_TIME=20000;                    // must be less than in GossipServer

    /**
     * a count of the number of servers contacted during getMembers() processing.
     * this is intended for use by the PingSender thread.  getMembers() should
     * be recoded to return this count and this variable should be removed
     */
    int responsiveServerCount; // GemStoneAddition
    int serversWithDistributedSystem; // GemStoneAddition
    boolean floatingCoordinatorDisabled; // GemStoneAddition
    private boolean networkPartitionDetectionEnabled; // GemStoneAddition
    private Address coordinator; // GemStoneAddition
    ProtocolStack stack; // GemStoneAddition
    int timeout = 5000; // GemStoneAddition - connection timeout


    protected final GemFireTracer log=GemFireTracer.getLog(this.getClass());
    
    private final Map<IpAddress, Integer> serverVersions = new HashMap<IpAddress, Integer>();


    /**
     * Creates the GossipClient
     * @param gossip_host The address and port of the host on which the GossipServer is running
     * @param expiry Interval (in msecs) for the refresher task
     */
    public GossipClient(IpAddress gossip_host, long expiry) {
        init(gossip_host, expiry);
    }


    /**
     Creates the GossipClient
     @param gossip_hosts List of IpAddresses
     @param expiry Interval (in msecs) for the refresher task
     */
    public GossipClient(Vector gossip_hosts, long expiry,
        ProtocolStack stack) { // GemStoneAddition - protocol stack
        if(gossip_hosts == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.GossipClient_EMPTY_SET_OF_GOSSIPSERVERS_GIVEN);
            return;
        }
        this.stack = stack;
        for(int i=0; i < gossip_hosts.size(); i++)
            init((IpAddress) gossip_hosts.elementAt(i), expiry);
        if (stack.gfPeerFunctions == null) {
          log.warn("jgroup membership manager is null!");
        }
        if (stack.gfPeerFunctions != null) {
          this.connectionTimer = stack.gfPeerFunctions.getConnectionTimeoutTimer();
        }
    }


    public void stop() {
        destroy(); // GemStoneAddition moved following 3 lines to a destroy() method
        //timer_running=false;
        //timer.cancel();
        //groups.clear();
        // provide another refresh tools in case the channel gets reconnected
        // TODO use SystemTimer?
        timer=new Timer(true); // GemStoneAddition - use daemon timer threads
        refresher_task=new Refresher();

    }


    public void destroy() { // GemStoneAddition
        timer_running=false;
        timer.cancel();
        groups.clear();
        if (connectionTimer != null) {
          connectionTimer.cancel();
        }
    }


    /**
     * Adds a GossipServer to be accessed.
     */
    public void addGossipServer(IpAddress gossip_host) {
      synchronized (gossip_servers) { // GemStoneAddition
        if(!gossip_servers.contains(gossip_host))
            gossip_servers.addElement(gossip_host);
      } // synchronized
    }
    
    /**
     * GemStoneAddition - set the timeout for the client
     * @param ms millisecond timeout period
     */
    public void setTimeout(int ms) {
      this.timeout = ms;
    }
    
    /**
     * GemStoneAddition - network partition detection support
     */
    public void setEnableNetworkPartitionDetection(boolean flag) {
      this.networkPartitionDetectionEnabled = flag;
    }


    /**
     Adds the member to the given group. If the group already has an entry for the member,
     its timestamp will be updated, preventing the cache cleaner from removing the entry.<p>
     The entry will be registered <em>with all GossipServers that GossipClient is configured to access</em>
     * @param inhibitRegistration
     */
    public void register(String group, Address mbr, long connectTimeout, boolean inhibitRegistration) {
        Vector mbrs;

        if(group == null || mbr == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.GossipClient_GROUP_OR_MBR_IS_NULL);
            return;
        }
        mbrs=(Vector) groups.get(group);
        if(mbrs == null) {
            mbrs=new Vector();
            mbrs.addElement(mbr);
            groups.put(group, mbrs);
        }
        else {
            if(!mbrs.contains(mbr))
                mbrs.addElement(mbr);
        }
        
        if (connectTimeout > 0) {
          this.timeout = (int)connectTimeout;  // GemStoneAddition - connection timeout
        }
        

        if (!inhibitRegistration) { // GemStoneAddition - registration inhibiting
          _register(group, mbr); // update entry in GossipServer
        }
        
        if (stack != null) {  // GemStoneAddition - pass stack to refresher so it can check connected state
          refresher_task.stack = stack;
        }

        if(!timer_running) {
          try { // GemStoneAddition - bug 36271
            timer.schedule(refresher_task, EXPIRY_TIME, EXPIRY_TIME);
            timer_running=true;
          }
          catch (IllegalStateException ise) {
            // timer has been cancelled, meaning we're shutting down
            return;
          }
        }
    }
    

    /**
     Returns all members of a given group
     @param group The group name
     @return Vector A list of Addresses
     */
    public Vector getMembers(String group
          ,Address localAddress, boolean logErrors, long connectTimeout) { // GemStoneAddition
        if(group == null) {
            if(log.isErrorEnabled()) log.error(ExternalStrings.GossipClient_GROUP_IS_NULL);
            return null;
        }
        this.timeout = (int)Math.min(Integer.MAX_VALUE, connectTimeout);

        return _getMembers(group, /* GemStoneAddition */localAddress, logErrors);
        //return _getMembers(group);
    }



    /**
     * GemStoneAddition
     * Returns the number of servers this client received a response from
     * the last time {@link #getMembers} was called.
     */
    public int getResponsiveServerCount() {
      return this.responsiveServerCount;
    }


    /* ------------------------------------- Private methods ----------------------------------- */


    void init(IpAddress gossip_host, long expiry) {
        EXPIRY_TIME=expiry;
        addGossipServer(gossip_host);
    }

    // @todo Parallelize GossipServer access
    /**
     * Registers the group|mbr with *all* GossipServers.
     * @return count of responsive servers (GemStoneAddition)
     */
    int _register(String group, Address mbr) {
        Socket sock;
        DataOutputStream out;
        IpAddress entry;
        GossipData gossip_req;
        Set<Address> serverAddresses = new HashSet<Address>();
        
        // GemStoneAddition - just a note to warn folks not to count responsive
        // servers, etc, in this method.  The registration timer task may run
        // during GET_MBRS processing, and we don't want to mess with the count
        // while that's happening.
//        this.responsiveServerCount = 0;
        int count = 0;

        // GemStoneAddition: use an iterator and synchronize
        synchronized (gossip_servers) {
          Vector newServers = new Vector();
          Iterator it = gossip_servers.iterator();
          while (it.hasNext()) {
            entry = (IpAddress)it.next();
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
//              if(log.isErrorEnabled()) log.error(JGroupsStrings.GossipClient_ENTRYHOST_OR_ENTRYPORT_IS_NULL);
                it.remove();
                continue;
            }
            try {
                if(log.isTraceEnabled())
                    log.trace("REGISTER_REQ --> " + entry.getIpAddress() + ':' + entry.getPort());
                //sock=new Socket(entry.getIpAddress(), entry.getPort());
                // GemStoneAddition - use SocketCreator
                if (JChannel.getGfFunctions().getSockCreator().isHostReachable(entry.getIpAddress())) {

                  // Get GemFire version from IPAddress first
                  _getVersionForAddress(entry);

                  ConnectTimerTask timeoutTask = new ConnectTimerTask(); // GemStoneAddition
                  sock = JChannel.getGfFunctions().getSockCreator().connect(
                      entry.getIpAddress(), entry.getPort(), 2 * this.timeout,
                      timeoutTask, false);
                  
                  out=new DataOutputStream(sock.getOutputStream());
                  gossip_req=new GossipData(GossipData.REGISTER_REQ, group, mbr, null, gossip_servers);
                  // must send GossipData as fast as possible, otherwise the
                  // request might be rejected
                  // GemStoneAddition - For rolling upgrade between old and new Gossiping members.
                  int gossipVersion = GossipServer.GOSSIPVERSION;
                  short serverOrdinal = JGroupsVersion.CURRENT_ORDINAL;
                  if (entry.getVersionOrdinal() <= JGroupsVersion.CURRENT_ORDINAL) {
                    // Get GOSSIPVERSION for receiving GossipServer
                    serverOrdinal = entry.getVersionOrdinal();
                    gossipVersion = JChannel.getGfFunctions().getGossipVersionForOrdinal(serverOrdinal);

                    // Change outputStream so any DataSerializable can handle GemFire versions.
                    out = JChannel.getGfFunctions().getVersionedDataOutputStream(out, serverOrdinal);
                  }
                  out.writeInt(gossipVersion); // GemStoneAddition
                  // We should send the GossipServer's version ordinal as GossipData
                  // is serialized based on GossipServer's version.
                  out.writeShort(serverOrdinal); // GemStoneAddition
                  JChannel.getGfFunctions().writeObject(gossip_req, out);
                  out.flush();

                  DataInputStream in=new DataInputStream(sock.getInputStream());
                  GossipData gossip_rsp = JChannel.getGfFunctions().readObject(in);
                  sock.close();
                  if (gossip_rsp.locators != null) {
                    newServers.addAll(gossip_rsp.locators);
                  }
                  if (gossip_rsp.localAddress != null) {
                    if (log.getLogWriter().fineEnabled()) {
                      log.getLogWriter().fine("locator " + entry + " member address is " + gossip_rsp.localAddress);
                    }
                    serverAddresses.add(gossip_rsp.localAddress);
                  }
                  count++;
                }
            }
            catch (java.net.SocketTimeoutException se) { // GemStoneAddition - socket connect attempt timed out (bug 38731)
              log.getLogWriter().info(ExternalStrings.GossipClient_ATTEMPT_TO_CONNECT_TO_DISTRIBUTION_LOCATOR_0_TIMED_OUT, entry);
            }
            catch (java.io.InterruptedIOException ie) { // GemStoneAddition - ping_sender is being shut down
                // don't log the event - see bug 34273
                //log.getLogWriter().severe("Could not connect to distribution locator " + entry + " (" + ie + ")");
                Thread.currentThread().interrupt();
                RuntimeException ex = JChannel.getGfFunctions().getDisconnectException("connection attempt interrupted"); // we're trying to exit
                ex.initCause(ie);
                throw ex;
            }
            catch (SSLException ex) {
                // GemStone addition for SSL sockets
                log.getLogWriter().info(
                    ExternalStrings.GossipClient_SSL_FAILURE_WHILE_CONNECTING_TO_DISTRIBUTION_LOCATOR_0,
                    entry);
                RuntimeException rex = JChannel.getGfFunctions().getAuthenticationFailedException(
                    ExternalStrings.GossipClient_SSL_FAILURE_WHILE_CONNECTING_TO_DISTRIBUTION_LOCATOR_0
                    .toLocalizedString(entry));
                rex.initCause(ex);
                throw rex;
            }
            catch(Exception ex) {
              Channel c = null; // added this to fix an NPE seen when running unit tests in eclipse
              if (stack != null) {
                c = stack.getChannel();
              }
              if (c != null && c.closing()) {
                destroy();
              }
              else {
                if (log.getLogWriter().fineEnabled()) {
                  log.getLogWriter().info(ExternalStrings.GossipClient_COULD_NOT_CONNECT_TO_DISTRIBUTION_LOCATOR__0, entry + ": " + ex);
                }
              }
                //if (log.isFatalEnabled()) log.fatal("exception connecting to locator " + entry + ": " + ex); // GemStoneAddition
            }
          }
          if (!newServers.isEmpty()) {
            GossipServer.processLocators(log, gossip_servers, newServers);
            synchronized(gossip_servers) {
              for (int i=0; i<newServers.size(); i++) {
                if (!gossip_servers.contains(newServers.get(i))) {
                  gossip_servers.add(newServers.get(i));
                }
              }
            }
          }
          this.serverAddresses = serverAddresses;
        } // synchronized
        return count;
    }

    /**
     * Sends a GET_MBR_REQ to *all* GossipServers, merges responses.
     */
    Vector _getMembers(String group
          ,Address localAddress, boolean logErrors) { // GemStoneAddition - added local address for atomic registration
        Vector ret=new Vector();
        Socket sock;
        DataOutputStream out;
        DataInputStream in;
        IpAddress entry;
        GossipData gossip_req, gossip_rsp;
        Address mbr;
        Set<Address> serverAddresses = new HashSet<Address>();
        int count = 0; // GemStoneAddition

        logErrors |= log.isTraceEnabled(); // GemStoneAddition
        this.serversWithDistributedSystem = 0; // GemStoneAddition
        // GemStoneAddition: use an iterator and synchronize
        synchronized (gossip_servers) {
          Vector newServers = new Vector();
//      for(int i=0; i < gossip_servers.size(); i++) {
        Iterator it = gossip_servers.iterator();
        while (it.hasNext()) {
          entry = (IpAddress)it.next();
          entry.setBirthViewId(0);
//            entry=(IpAddress) gossip_servers.elementAt(i);
            if(entry.getIpAddress() == null || entry.getPort() == 0) {
//              if(logErrors && log.isErrorEnabled()) log.error(JGroupsStrings.GossipClient_ENTRYHOST_OR_ENTRYPORT_IS_NULL);
                it.remove();
                continue;
            }
            try {

                if(log.isTraceEnabled()) log.trace("GET_REQ --> " + entry.getIpAddress() + ':' + entry.getPort());
                // Get GemFire version from IPAddress first
                _getVersionForAddress(entry);

                // GemStoneAddition - use SocketCreator
                ConnectTimerTask timeoutTask = new ConnectTimerTask(); // GemStoneAddition
                sock=JChannel.getGfFunctions().getSockCreator().connect(
                    entry.getIpAddress(), entry.getPort(), this.timeout,
                    timeoutTask, false);
                //sock=new Socket(entry.getIpAddress(), entry.getPort());
                if (this.timeout > 0) { // GemStoneAddition - don't wait forever (bug #43627)
                  sock.setSoTimeout(3 * this.timeout); // this can take a while, so use a multiple of the member-timeout setting
                }
                
                out=new DataOutputStream(sock.getOutputStream());

                gossip_req=new GossipData(GossipData.GET_REQ, group, localAddress, null, gossip_servers);
                // must send GossipData as fast as possible, otherwise the
                // request might be rejected
                // GemStoneAddition - For rolling upgrade between old and new Gossiping members.
                int gossipVersion = GossipServer.GOSSIPVERSION;
                short serverOrdinal = JGroupsVersion.CURRENT_ORDINAL;
                if (entry.getVersionOrdinal() <= JGroupsVersion.CURRENT_ORDINAL) {
                  // Get GOSSIPVERSION for receiving GossipServer
                  serverOrdinal = entry.getVersionOrdinal();
                  gossipVersion = JChannel.getGfFunctions().getGossipVersionForOrdinal(serverOrdinal);

                  // Change outputStream so any DataSerializable can handle GemFire versions.
                  out = JChannel.getGfFunctions().getVersionedDataOutputStream(out, serverOrdinal); 
                }
              
                out.writeInt(gossipVersion); // GemStoneAddition
                // We should send the GossipServer's version ordinal as GossipData
                // is serialized based on GossipServer's version.
                out.writeShort(serverOrdinal); // GemStoneAddition
                JChannel.getGfFunctions().writeObject(gossip_req, out);
                out.flush();

                in=new DataInputStream(sock.getInputStream());
                if (serverOrdinal < JGroupsVersion.CURRENT_ORDINAL) {
                  in = JChannel.getGfFunctions().getVersionedDataInputStream(in, serverOrdinal);
                }
                gossip_rsp= JChannel.getGfFunctions().readObject(in);
                // GemStoneAddition
                if (gossip_rsp.getHasDistributedSystem()) {
                  this.serversWithDistributedSystem++;
                }
                if (gossip_rsp.getFloatingCoordinatorDisabled()) {
                  this.floatingCoordinatorDisabled = true;
                }
                if (gossip_rsp.getNetworkPartitionDetectionEnabled()) {
                  this.networkPartitionDetectionEnabled = true;
                }
                if (gossip_rsp.locators != null && !gossip_rsp.locators.isEmpty()) {
                  newServers.addAll(gossip_rsp.locators);
                }
                if(gossip_rsp.mbrs != null) { // merge with ret
                    for(int j=0; j < gossip_rsp.mbrs.size(); j++) {
                        mbr=(Address) gossip_rsp.mbrs.get(j);
                        if(!ret.contains(mbr))
                            ret.addElement(mbr);
                    }
                }
                
                if (gossip_rsp.mbr != null) { // GemStoneAddition
                  this.coordinator = gossip_rsp.mbr;
                }
                
                if (group.equals(GossipServer.CHANNEL_NAME) && gossip_rsp.localAddress != null) {
                  log.getLogWriter().info(ExternalStrings.DEBUG, "locator " + entry + " member address is " + gossip_rsp.localAddress);
                  serverAddresses.add(gossip_rsp.localAddress);
                  if (!ret.contains(gossip_rsp.localAddress)) {
                    ret.addElement(gossip_rsp.localAddress);
                  }
                }

                count++; // GemStoneAddition
                sock.close();
            }
            catch(Exception ex) {
//                if (DistributionManager.getDistributionManagerType() != 
//                       DistributionManager.LOCATOR_DM_TYPE) {  // [GemStone]
                  if (logErrors) {
                    if (JChannel.getGfFunctions().getSockCreator().useSSL()) {
                      log.getLogWriter().info(ExternalStrings.GossipClient_UNABLE_TO_CONNECT_TO_LOCATOR__0, entry, ex);
                    }
                    else {
                      log.getLogWriter().info(ExternalStrings.GossipClient_UNABLE_TO_CONNECT_TO_LOCATOR__0, entry);
                    }
                  }
//                }
            }
        }
        if (!newServers.isEmpty()) {
          GossipServer.processLocators(log, gossip_servers, newServers);
          synchronized(gossip_servers) {
            for (int i=0; i<newServers.size(); i++) {
              if (!gossip_servers.contains(newServers.get(i))) {
                gossip_servers.add(newServers.get(i));
              }
            }
          }
        }
        this.serverAddresses = serverAddresses;
        } // synchronized

        if (log.isDebugEnabled()) {
          log.debug("getMbrs returning results from " + count
              + " locators");
        }
          
        this.responsiveServerCount = count;
        return ret;
    }

  /**
   * GemStoneAddition - Get version for an IpAddress from a known gossip server.
   *
   * @throws ClassNotFoundException
   * @throws IOException
   */
  private IpAddress _getVersionForAddress(Address addr)
          throws ClassNotFoundException, IOException {
    IpAddress entry = (IpAddress) addr;

    Integer versionOrdinal;
    synchronized(serverVersions) {
      versionOrdinal = serverVersions.get(addr);
    }
    if (versionOrdinal == null) {
      if (log.isTraceEnabled())
        log.trace("GEMFIRE_VERSION --> " + entry.getIpAddress() + ':'
                + +entry.getPort());
      if (JChannel.getGfFunctions().getSockCreator()
              .isHostReachable(entry.getIpAddress())) {
  
        ConnectTimerTask timeoutTask = new ConnectTimerTask(); // GemStoneAddition
        Socket socket = JChannel.getGfFunctions().getSockCreator().connect(
                entry.getIpAddress(), entry.getPort(), 2 * this.timeout,
                timeoutTask, false);
  
        if (this.timeout > 0) { // GemStoneAddition - don't wait forever (bug #43627)
          socket.setSoTimeout(3 * this.timeout); // this can take a while, so use a multiple of the member-timeout setting
        }
  
        // Get GemFire version from IPAddress first
        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
        // out = new VersionedDataOutputStream(out, Version.CURRENT);
        GossipData version_req = new GossipData(GossipData.GEMFIRE_VERSION, null,
                null, null, null);
  
        // Need to send previous (older) GOSSIPVERSION so that TcpServer (Older or
        // newer which we don't know yet) doesn't reject it.
        int prevGossipVersion = GossipServer.OLDGOSSIPVERSION;
        out.writeInt(prevGossipVersion); // GemStoneAddition
        JChannel.getGfFunctions().writeObject(version_req, out);
        out.flush();
  
        DataInputStream in = new DataInputStream(socket.getInputStream());
        GossipData gossip_rsp = JChannel.getGfFunctions().readObject(in);
        
        if (log.isTraceEnabled() && gossip_rsp.versionOrdinal != JGroupsVersion.CURRENT_ORDINAL) {
          log.trace("Gossip response with version " + gossip_rsp);
        }
        
        versionOrdinal = Integer.valueOf(gossip_rsp.versionOrdinal);
        synchronized(serverVersions) {
          serverVersions.put(entry, versionOrdinal);
        }
        
        socket.close();
      }
    }

    entry.setVersionOrdinal((short)versionOrdinal.intValue());
    
    return entry;
  }


  /** GemStoneAddition - return the number of locators that have
     *  a connected DistributedSystem.  This information is gathered
     *  during getMembers processing
     */
    public int getServerDistributedSystemCount() {
      return this.serversWithDistributedSystem;
    }
    
    /**
     * GemStoneAddition - return the flag stating whether the membership
     * coordinator role must be colocated with a GossipServer
     */
    public boolean getFloatingCoordinatorDisabled() {
      return this.floatingCoordinatorDisabled;
    }
    
    /**
     * GemStoneAddition - return the flag stating whether the locator
     * has network-partition-detection-enabled == true
     */
    public boolean getNetworkPartitionDetectionEnabled() {
      return this.networkPartitionDetectionEnabled;
    }
    
    /* ---------------------------------- End of Private methods ------------------------------- */



    /**
     * Periodically iterates through groups and refreshes all registrations with GossipServer
     */
    class Refresher extends TimerTask {

      public ProtocolStack stack;  // GemStoneAddition

      @Override // GemStoneAddition
        public void run() {
            int num_items=0;
            String group;
            Vector mbrs;
            Address mbr;

          if(log.isTraceEnabled()) log.trace("refresher task starting");
          if (stack == null || stack.getChannel().isConnected()) { // GemStoneAddition fix for bug #42076
            for(Enumeration e=groups.keys(); e.hasMoreElements();) {
                group=(String) e.nextElement();
                mbrs=(Vector) groups.get(group);
                if(mbrs != null) {
                    for(int i=0; i < mbrs.size(); i++) {
                        mbr=(Address) mbrs.elementAt(i);
                        if(log.isTraceEnabled()) log.trace("registering " + group + " : " + mbr);
                        try { // GemStoneAddition - symptom in bug 38731
                          register(group, mbr, 0, false);
                        }
                        catch (RuntimeException ex) {
                          GossipClient.this.timer.cancel();
                          return;
                        }
                        num_items++;
                    }
                }
            }
            if(log.isTraceEnabled()) log.trace("refresher task done. Registered " + num_items + " items");
          } else {
            if (log.isTraceEnabled()) log.trace("refresher task done.  Not connected so no registration performed");
          }
        }

    }


    public static void main(String[] args) {
        Vector gossip_hosts=new Vector();
        String host;
        InetAddress ip_addr;
        int port;
        boolean get=false, register=false, keep_running=false;
        String register_host=null;
        int register_port=0;
        String get_group=null, register_group=null;
        GossipClient gossip_client=null;
        Vector mbrs;
        long expiry=20000;


        for(int i=0; i < args.length; i++) {
            if("-help".equals(args[i])) {
                usage();
                return;
            }
            if("-expiry".equals(args[i])) {
                expiry=Long.parseLong(args[++i]);
                continue;
            }
            if("-host".equals(args[i])) {
                host=args[++i];
                port=Integer.parseInt(args[++i]);
                try {
                    ip_addr=InetAddress.getByName(host);
                    gossip_hosts.addElement(new IpAddress(ip_addr, port));
                }
                catch(Exception ex) {
                    System.err.println(ex);
                }
                continue;
            }
            if("-keep_running".equals(args[i])) {
                keep_running=true;
                continue;
            }
            if("-get".equals(args[i])) {
                get=true;
                get_group=args[++i];
                continue;
            }
            if("-register".equals(args[i])) {
                register_group=args[++i];
                register_host=args[++i];
                register_port=Integer.parseInt(args[++i]);
                register=true;
                continue;
            }
            usage();
            return;
        }

        if(gossip_hosts.size() == 0) {
            System.err.println("At least 1 GossipServer has to be given");
            return;
        }

        if(!register && !get) {
            System.err.println("Neither get nor register command given, will not do anything");
            return;
        }

        try {

        }
        catch (VirtualMachineError err) { // GemStoneAddition
          // If this ever returns, rethrow the error.  We're poisoned
          // now, so don't let this thread continue.
          throw err;
        }
        catch(Throwable ex) {
            System.err.println("GossipClient.main(): error initailizing JGroups Trace: " + ex);
        }

        try {
            gossip_client=new GossipClient(gossip_hosts, expiry, null);
            if(register) {
                System.out.println("Registering " + register_group + " --> " + register_host + ':' + register_port);
                gossip_client.register(register_group, new IpAddress(register_host, register_port), 0, false);
            }

            if(get) {
                System.out.println("Getting members for group " + get_group);
                mbrs=gossip_client.getMembers(get_group, null, true, 0);
                System.out.println("Members for group " + get_group + " are " + mbrs);
            }
        }
        catch(Exception ex) {
            System.err.println(ex);
        }
        if(!keep_running)
            gossip_client.stop();
    }


    static void usage() {
        System.out.println("GossipClient [-help] [-host <hostname> <port>]+ " +
                           " [-get <groupname>] [-register <groupname hostname port>] [-expiry <msecs>] " +
                           "[-keep_running]]");
    }
   
  /** GemStoneAddition - an object to monitor connection progress */
  class ConnectTimerTask extends TimerTask implements ConnectionWatcher {
    Socket watchedSocket;
    volatile boolean cancelled;
    
    public void beforeConnect(Socket sock) {
      watchedSocket = sock;
      if (connectionTimer != null) {
        connectionTimer.schedule(this, timeout);
      } else {
        timer.schedule(this, timeout);
      }
    }
    
    public void afterConnect(Socket sock) {
//      if (watchedSocket.isClosed()) {
//        log.getLogWriter().info("DEBUG: socket already closed in afterConnect");
//      }
      cancelled = true;
      this.cancel();
    }
    
    @Override // GemStoneAddition
    public void run() {
      if (!cancelled) {
        this.cancel();
        try {
          log.getLogWriter().fine("Timing out attempted connection to locator");
          watchedSocket.close();
        }
        catch (IOException e) {
          // unable to close the socket - give up
        }
      }
    }
  
  }


  public Address getCoordinator() {
    return this.coordinator;
  }


}
