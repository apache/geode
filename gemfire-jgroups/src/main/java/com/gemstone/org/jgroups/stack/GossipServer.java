/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GossipServer.java,v 1.10 2005/06/09 18:31:02 belaban Exp $

package com.gemstone.org.jgroups.stack;

import java.io.Externalizable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.protocols.TCPGOSSIP;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;


/**
 * Maintains a cache of member addresses for each group. There are essentially 2 functions: get the members for
 * a given group and register a new member for a given group. Clients have to periodically renew their
 * registrations (like in JINI leasing), otherwise the cache will be cleaned periodically (oldest entries first).<p>
 * The server should be running at a well-known port. This can be done by for example adding an entry to
 * /etc/inetd.conf on UNIX systems, e.g. <code>gossipsrv stream tcp nowait root /bin/start-gossip-server</code>.
 * <code>gossipsrv</code> has to be defined in /etc/services and <code>start-gossip-server</code> is a script
 * which starts the GossipServer at the well-known port (define in /etc/services). The protocol between GossipServer
 * and GossipClient consists of REGISTER_REQ, GET_MEMBERS_REQ and GET_MEMBERS_RSP protocol data units.<p>
 * The server does not spawn a thread/request, but does all of its processing on the main thread. This should not
 * be a problem as all requests are short-lived. However, the server would essentially cease processing requests
 * if a telnet connected to it.<p>
 * Requires JDK >= 1.3 due to the use of Timer
 * deprecated Use GossipRouter instead // GemStoneAddition - remove deprecation of GossipServer for now.  The router has too much baggage.
 * @author Bela Ban Oct 4 2001
 * 
 */
public abstract class GossipServer {
  
  public final static int GOSSIPVERSION = 1002;
  // Don't change it ever. We did NOT send GemFire version in a Gossip request till 1001 version.
  // This GOSSIPVERSION is used in _getVersionForAddress request for getting GemFire version of a GossipServer.
  public final static int OLDGOSSIPVERSION = 1001;

  public final static boolean LOCATOR_DISCOVERY_DISABLED = Boolean.getBoolean("gemfire.disable-locator-discovery");
    
    // change this name to prevent gemfire from connecting to versions having a different name
    public static final String CHANNEL_NAME = "GF7";
    
    private static GossipServer instance; // GemStoneAddition: singleton class
    
    /**
     * GemStoneAddition (comment)
     * <p>
     * Table where the keys are {@link String}s (group names) and the
     * values are {@link List}s of {@link com.gemstone.org.jgroups.stack.GossipServer.Entry}s.
     * <p>
     * Since <code>GossipServer.CacheCleaner</code> accesses this object concurrently,
     * updates to this field must be synchronized on the instance.
     * 
     * @see #sweep()
     * @see #addMember(String, Address)
     */
    protected final Map groups=new HashMap();  // groupname - list of Entry's
    static long EXPIRY_TIME_DEFAULT = 30000; // GemStoneAddition
    protected long expiry_time=EXPIRY_TIME_DEFAULT;       // time (in msecs) until a cache entry expires
    protected CacheCleaner cache_cleaner=null;      // task that is periodically invoked to sweep old entries from the cache
    // TODO use SystemTimer?
    protected Timer timer;   // start as daemon thread, so we won't block on it upon termination
    
    protected final GemFireTracer log=GemFireTracer.getLog(getClass());
    
    protected boolean floatingCoordinatorDisabled; // GemStoneAddition
    protected boolean networkPartitionDetectionEnabled; // GemStoneAddition
    protected InetAddress bind_address;
    protected int port;
    protected String locatorString;

    /** GemStoneAddition - the current membership coordinator */
    protected Address coordinator;
    protected Vector locators;
    protected Address localAddress; // added in 8.0 for bug #30341
    protected Object localAddressSync = new Object();
    protected boolean withDS; // true if there will be a distributed system

    
    public static GossipServer getInstance() {
      synchronized(GossipServer.class) {
        return instance;
      }
    }

    public GossipServer(int port, long expiry_time,
        InetAddress bind_address,
        String locatorString,
        boolean floatingCoordinatorDisabled,
        boolean networkPartitionDetectionEnabled,
        boolean withDS) {
      this.port = port;
      this.bind_address = bind_address;
      this.expiry_time=expiry_time;
      this.floatingCoordinatorDisabled = floatingCoordinatorDisabled;
      this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
      this.locatorString = locatorString;
      this.withDS = withDS;
      if (this.locatorString == null || this.locatorString.length() == 0) {
        this.locators = new Vector();
      } else {
        this.locators = TCPGOSSIP.createInitialHosts(this.locatorString);
      }
    }
    
    public void init() {
      synchronized(GossipServer.class) {
        GossipServer.instance = this;
      }
    }

    /** recover gossip state from another locator or from the given stateFile
     * 
     * @param sFile
     *          a file containing gossipserver state from a past run
     * @param locatorsString
     *          a GemFire distributed system locators string
     */
    public abstract void recover(File sFile, String locatorsString);
    
    /* ----------------------------------- Private methods ----------------------------------- */

    /**
     * compare the given list with the locators vector.  If there are
     * any new ones, add them.
     * 
     * @param myLocators the existing locators
     * @param otherLocators the locators received from someone else
     * @return true if otherLocators has different locators than myLocators
     */
    public static boolean processLocators(GemFireTracer log, Vector myLocators, Vector otherLocators) {
      if (LOCATOR_DISCOVERY_DISABLED) {
        return false;
      }
      if (otherLocators == null) {
        return true;
      }
      synchronized(myLocators) {
        List newLocators = null;
        for (Iterator e = otherLocators.iterator(); e.hasNext(); ) {
          Address a = (Address)e.next();
          if (!myLocators.contains(a)) {
            if (newLocators == null) {
              newLocators = new LinkedList();
            }
            newLocators.add(a);
          }
        }
        if (newLocators != null) {
          myLocators.addAll(newLocators);
          return true;
        } else {
          return myLocators.size() == otherLocators.size();
        }
      }
    }

    /**
     Process the gossip request. Return a gossip response or null if none.
     */
    public synchronized Object processRequest(Object request) {
      if(!(request instanceof GossipData)) {
        throw new IllegalStateException("Expected gossipData, got " + request.getClass());
      }
      GossipData gossip = (GossipData) request;
        String group;
        Address mbr = null;

//        if(gossip == null) return null; GemStoneAddition not possible
        if(log.isDebugEnabled()) log.trace("processing request " + gossip.toString()); // GemStoneAddition - changed to trace()
        switch(gossip.getType()) {
            case GossipData.REGISTER_REQ:
                group=gossip.getGroup();
                mbr=gossip.getMbr();
                if(group == null || mbr == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.GossipServer_GROUP_OR_MEMBER_IS_NULL_CANNOT_REGISTER_MEMBER);
                    return null;
                }
                boolean differed = processLocators(log, locators, gossip.locators);
                return processRegisterRequest(group, mbr, differed);

            case GossipData.GET_REQ:
                group=gossip.getGroup();
                mbr = gossip.getMbr();
                if(group == null) {
                    if(log.isErrorEnabled()) log.error(ExternalStrings.GossipServer_GROUP_IS_NULL_CANNOT_GET_MEMBERSHIP);
                    return null;
                }
                differed = processLocators(log, locators, gossip.locators);
                return processGetRequest(group, mbr, differed); // GemStoneAddition - add mbr to group on a get req to make atomic

            case GossipData.GEMFIRE_VERSION:
              return processVersionRequest(); // GemStoneAddition - add mbr to group on a get req to make atomic

            case GossipData.GET_RSP:  // should not be received
                if(log.isWarnEnabled()) log.warn(ExternalStrings.GossipServer_RECEIVED_A_GET_RSP_SHOULD_NOT_BE_RECEIVED_BY_SERVER);
                return null;
	    
            default:
                if(log.isWarnEnabled()) log.warn(
                    ExternalStrings.GossipServer_RECEIVED_UNKOWN_GOSSIP_REQUEST_GOSSIP_0
                    .toLocalizedString(gossip));
                return null;
        }
    }

    
    public void endRequest(Object request,long startTime) { }
    public void endResponse(Object request,long startTime) { }
    
    /**
     * GemStoneAddition - internet protocol version compatibility check
     * @param addr the address of the other member
     * @return true if the member is using the same version of IP we are
     */
    private boolean checkCompatibility(Address addr) {
      if (this.bind_address != null && addr != null) {
        return this.bind_address.getClass() == ((IpAddress)addr).getIpAddress().getClass();
      }
      return true;
    }

    GossipData processRegisterRequest(String group, Address mbr, boolean sendLocators) {
        if (!checkCompatibility(mbr)) {
          log.getLogWriter().warning(ExternalStrings.GossipServer_RECEIVED_REGISTRATION_REQUEST_FROM_MEMBER_USING_INCOMPATIBLE_INTERNET_PROTOCOL_0, mbr);
        }
        addMember(group, mbr);
        persistState();
        GossipData rsp = new GossipData();
        if (sendLocators) {
          rsp.locators = new Vector(this.locators);
        }

        // bug #30341 - wait until the local address is known before replying to a registration
        if (withDS) {
          try {
            synchronized(this.localAddressSync) {
              while (this.localAddress == null /*&& this.hasDistributedSystem()*/) {
                this.localAddressSync.wait();
              }
            }
          } catch (InterruptedException e) {
            return null;
          }
        }
        rsp.localAddress = this.localAddress;
        
        return rsp;
    }

    // GemStoneAddition - added mbr parameter.  Member is added to group on
    //            a get() request to make it atomic.  Bug 30341
    GossipData processGetRequest(String group, Address mbr, boolean sendLocators) {
        if (!checkCompatibility(mbr)) {
          log.getLogWriter().warning(ExternalStrings.GossipServer_RECEIVED_GETMEMBERS_REQUEST_FROM_MEMBER_USING_INCOMPATIBLE_INTERNET_PROTOCOL_0, mbr);
        }
        GossipData ret=null;
        List mbrs;
        synchronized (this) { // fix for bug 30341
          mbrs=getMembers(group);
          if (mbr != null) {
            addMember(group, mbr);
            persistState();
          }
        }
        
        if (withDS) {
          // bug #30341 - wait until the local address is known before replying to a registration
          try {
            synchronized(this.localAddressSync) {
              while (this.localAddress == null /*&& this.hasDistributedSystem()*/) {
                this.localAddressSync.wait();
              }
            }
          } catch (InterruptedException e) {
            return null;
          }
        }
        
        ret=new GossipData(GossipData.GET_RSP, group, this.coordinator, mbrs,
            this.hasDistributedSystem(), this.floatingCoordinatorDisabled,
            this.networkPartitionDetectionEnabled, (sendLocators?this.locators:null), this.localAddress);

        if(log.isTraceEnabled()) {
          log.trace("get-members response = " + ret); // GemStoneAddition
        }
        
        return ret;
    }

    GossipData processVersionRequest() {
      
      GossipData ret = new GossipData(GossipData.GEMFIRE_VERSION, null, null, null, null);
      ret.versionOrdinal = JGroupsVersion.CURRENT_ORDINAL;

      if (log.getLogWriter().fineEnabled()) {
        log.getLogWriter().fine(
            "version response = " + ret.versionOrdinal); // GemStoneAddition
      }
      
      return ret;
    }

    public abstract void persistState();


    /**
     * note that it is okay for this to return false if this locator will have a
     * DS but it has not yet been created
     * @return true if there is a distributed system running in this vm
     */
    public abstract boolean hasDistributedSystem();
    
    /**
     Adds a member to the list for the given group. If the group doesn't exist, it will be created. If the member
     is already present, its timestamp will be updated. Otherwise the member will be added.
     @param group The group name. Guaranteed to be non-null
     @param mbr The member's address. Guaranteed to be non-null
     */
    void addMember(String group, Address mbr) {
        List mbrs;
        synchronized (groups) { // GemStoneAddition
          mbrs =(List) groups.get(group);
        }
        Entry entry;

        if(mbrs == null) {
            mbrs=newEntryList();
            mbrs.add(newEntry(mbr));
            synchronized (groups) { // GemStoneAddition
              groups.put(group, mbrs);
            }
            //GemStoneAddition - set to Trace level
             if(log.isTraceEnabled()) log.trace("added " + mbr + " to discovery set for " + group + " (new group)");
        }
        else {
            entry=findEntry(mbrs, mbr);
            if(entry == null) {
                entry=newEntry(mbr);
                synchronized (mbrs) {
                  mbrs.add(entry);
                }
                //GemStoneAddition - set to Trace level
                 if(log.isTraceEnabled()) log.trace("added " + mbr + " to discovery set for " + group);
            }
            else {
                entry.mbr = mbr;
                entry.update();
                //GemStoneAddition - set to Trace level
                 if(log.isTraceEnabled()) log.trace("updated discovery set entry " + entry);
            }
        }
    }
    
    public abstract EntryList newEntryList();
    
    public abstract Entry newEntry(Address mbr);


    /** test hook for GemFire - see how many members are in the discovery set */
    public int getMemberCount() {
      List mbrs = getMembers(CHANNEL_NAME);
      if (mbrs == null) {
        return -1;
      }
      return mbrs.size();
    }
    
    
    List getMembers(String group) {
        List ret=null;
        List mbrs;
        synchronized (groups) { // GemStoneAddition
          mbrs =(List) groups.get(group);
        }

        if(mbrs == null)
            return null;
        synchronized (mbrs) {
          ret=new ArrayList(mbrs.size());
          for(int i=0; i < mbrs.size(); i++)
            ret.add(((Entry) mbrs.get(i)).mbr);
        }
        return ret;
    }


    Entry findEntry(List mbrs, Address mbr) {
        Entry entry=null;

        synchronized (mbrs) { // GemStoneAddition
          for(int i=0; i < mbrs.size(); i++) {
            entry=(Entry) mbrs.get(i);
            if(entry.mbr != null && entry.mbr.equals(mbr))
                return entry;
          }
        }
        return null;
    }


    /**
     * Remove expired entries (entries older than EXPIRY_TIME msec).
     */
    synchronized void sweep() {
        long current_time=System.currentTimeMillis(), diff;
        int num_entries_removed=0;
        String key=null;
        List val;
        Entry entry;

      boolean done; // GemStoneAddition - fix for bug 31361
      do {
        done = true;
        try {
  
          for(Iterator e=groups.keySet().iterator(); e.hasNext();) {
              key=(String) e.next();
              val=(List) groups.get(key);
              if(val != null) {
                // GemStoneAddition -- don't use iterator.  It is live
                // and can fail due to concurrent modification.  Instead,
                // safely create a copy of the list and iterate over _that_
                ArrayList listCopy;
                synchronized (val) {
                  listCopy = new ArrayList(val);
                }
                  for(Iterator it=listCopy.iterator() /*val.listIterator()*/; it.hasNext();) {
                      entry=(Entry) it.next();
                      diff=current_time - entry.timestamp;
                      if(entry.timestamp + expiry_time < current_time) {
//                          it.remove();
                          val.remove(entry);
                          if(log.isTraceEnabled()) log.trace("removed member " + entry +
                                                                 " from discovery set " + key + '(' + diff + " msecs old)");
                          num_entries_removed++;
                      }
                  }
              }
          }
  
        } catch (ConcurrentModificationException ex) {
          done = false;
        }
      } while (!done);

      if(num_entries_removed > 0)
        if(log.isInfoEnabled()) log.info(ExternalStrings.GossipServer_DONE_REMOVED__0__DISCOVERY_ENTRIES, num_entries_removed);
    }
    
    public void shutDown() {
      this.timer.cancel();
    }

    /* -------------------------------- End of Private methods ----------------------------------- */

    /**
     *   Maintains the member address plus a timestamp. Used by CacheCleaner thread to remove old entries.
     */
    public static class Entry {
        public Address mbr=null;
        public long timestamp=0;

        public Entry(Address mbr) {
            this.mbr=mbr;
            update();
        }

        public void update() {
            timestamp=System.currentTimeMillis();
        }

        @Override // GemStoneAddition
        public boolean equals(Object other) {
          // GemStoneAddition - this was assuming the argument was an address,
          // which caused removals on the containing collection to fail
            if(mbr != null && other != null) {
              if (other instanceof Address) {
                return mbr.equals(other);
              }
              else if (other instanceof Entry) {
                return mbr.equals(((Entry)other).mbr);
              }
            }
            return false;
        }

        @Override // GemStoneAddition
        public int hashCode() { // GemStoneAddition
          return 0; // TODO more efficient implementation :-)
        }
        
        @Override // GemStoneAddition
        public String toString() {
            return "mbr=" + mbr;
        }

        public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
          this.mbr = new IpAddress();
          this.mbr.readExternal(in);
          update();
        }

        public void writeExternal(ObjectOutput out) throws IOException {
          mbr.writeExternal(out);
        }
    }

    // GemStoneAddition
    public static class EntryList extends ArrayList<Entry> {
      private static final long serialVersionUID = -3718320921705914128L;
      public EntryList() {
      }
      public EntryList(int size) {
        super(size);
      }
    }


    /**
     * Periodically sweeps the cache and removes old items (items that are older than EXPIRY_TIME msecs)
     */
    public class CacheCleaner extends TimerTask  {

      @Override // GemStoneAddition
        public void run() {
            sweep();
        }

    }

    /**
     * @param coordinator the coordinator to set
     */
    public void setCoordinator(Address coordinator) {
      this.coordinator = coordinator;
    }
    
    /**
     * @param address the address of this member
     */
    public void setLocalAddress(Address address) {
      synchronized(this.localAddressSync) {
        this.localAddress = address;
        this.localAddressSync.notifyAll();
      }
    }
    
    /**  GemStoneAddition - we don't want this main method
    public static void main(String[] args)
            throws java.net.UnknownHostException {
        String arg;
        int port=7500;
        long expiry_time=30000;
        GossipServer gossip_server=null;
        InetAddress address=null;
        for(int i=0; i < args.length; i++) {
            arg=args[i];
            if("-help".equals(arg)) {
                System.out.println("GossipServer [-port <port>] [-expiry <msecs>] [-bindaddress <address>]");
                return;
            }
            if("-port".equals(arg)) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-expiry".equals(arg)) {
                expiry_time=Long.parseLong(args[++i]);
                continue;
            }
            if("-bindaddress".equals(arg)) {
                address=InetAddress.getByName(args[++i]);
                continue;
            }
            System.out.println("GossipServer [-port <port>] [-expiry <msecs>]");
            return;
        }

        try {

        }
        catch(Throwable ex) {
            System.err.println("GossipServer.main(): " + ex);
        }

        try {
            gossip_server=new GossipServer(port, expiry_time, address);
            gossip_server.run();
        }
        catch(Exception e) {
            System.err.println("GossipServer.main(): " + e);
        }
    }
    */

    public static int getCurrentGossipVersion() {
      return GOSSIPVERSION;
    }

    public static int getOldGossipVersion() {
      return OLDGOSSIPVERSION;
    }


}
