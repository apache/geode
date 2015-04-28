package com.gemstone.gemfire.distributed.internal.membership.jgroup;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Timer;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.i18n.StringIdImpl;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.VersionedObjectOutput;
import com.gemstone.gemfire.internal.admin.remote.DistributionLocatorId;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.stack.GossipClient;
import com.gemstone.org.jgroups.stack.GossipServer;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GemFireTracer;


/**
 * LocatorHandler extends the JGroup GossipServer to provide persistence and
 * recovery for the discovery set.
 */
public class LocatorImpl extends GossipServer implements TcpHandler {
  
  public static int FILE_FORMAT_VERSION = 1004; // added ip address
  private static/* GemStoneAddition */ final Map<Integer, Integer> FILE_FORMAT_TO_GEMFIRE_VERSION_MAP = new HashMap<Integer, Integer>();

  protected final GemFireTracer log=GemFireTracer.getLog(getClass());

  private File stateFile;

    /**
     * GemStoneAddition - Initialize versions map.
     * Warning: This map must be compatible with all GemFire versions being
     * handled by this member "With different GOSSIPVERION". If GOSSIPVERIONS
     * are same for then current GOSSIPVERSION should be used.
     *
     * @since 7.1
     */
    static {
      FILE_FORMAT_TO_GEMFIRE_VERSION_MAP.put(1003, (int)JGroupsVersion.GFE_701_ORDINAL);
      FILE_FORMAT_TO_GEMFIRE_VERSION_MAP.put(1004, (int)JGroupsVersion.CURRENT_ORDINAL);
    }

    /**
     * 
     * @param port              number of the tcp/ip server socket port to use
     * @param expiry_time       time until registration entries expire
     * @param bind_address      network address to bind to
     * @param stateFile         name of the file to persist state to/recover from
     * @param locatorString     location of other locators (bootstrapping, failover)
     * @param floatingCoordinatorDisabled       true if Coordinator can only be in Locators
     * @param networkPartitionDetectionEnabled true if network partition detection is enabled
     * @param withDS            true if a distributed system has been or will be started
     */
    public LocatorImpl(int port, long expiry_time,
                        InetAddress bind_address,
                        File stateFile,
                        String locatorString,
                        boolean floatingCoordinatorDisabled,
                        boolean networkPartitionDetectionEnabled,
                        boolean withDS)
    {
      super(port, expiry_time, bind_address, locatorString, floatingCoordinatorDisabled, networkPartitionDetectionEnabled, withDS);
      this.stateFile = stateFile;
    }
    
    public void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig) {
    }

    
    /** recover gossip state from another locator or from the given stateFile
     * 
     * @param sFile
     *          a file containing gossipserver state from a past run
     * @param locatorsString
     *          a GemFire distributed system locators string
     */
    public void recover(File sFile, String locatorsString) {
      // First recover from file to find out correct versions of GossipServers 
      recoverFromFile(sFile);
      // Send getMembers request with correct GemFire version number.
      recoverFromOthers(locatorsString);
    }
    
    /** GemStoneAddition - get gossip state from another locator.  Return
     * true if we're able to contact another locator and get its state.
     * @param locatorsString
     *   a string with locator specs in GemFire form
     */
    private boolean recoverFromOthers(String locatorsString) {
      String myAddr;
      String myAddr2 = null;
      if (locatorsString != null && locatorsString.length() > 0) {
        if (this.bind_address == null) {
          try {
            myAddr = SocketCreator.getLocalHost().getHostName();
            myAddr2 = SocketCreator.getLocalHost().getCanonicalHostName();
          }
          catch (UnknownHostException ue) {
            log.getLogWriter().warning(ExternalStrings.GossipServer_UNABLE_TO_RESOLVE_LOCAL_HOST_NAME, ue);
            myAddr = "localhost";
          }
        }
        else {
          myAddr = this.bind_address.getHostAddress();
          myAddr2 = this.bind_address.getCanonicalHostName();
        }
        StringTokenizer st = new StringTokenizer(locatorsString, ",");
        while (st.hasMoreTokens()) {
          String l = st.nextToken();
          DistributionLocatorId locId = new DistributionLocatorId(l);
          if (!locId.isMcastId()) {
            String otherAddr = locId.getBindAddress();
            String otherAddr2 = null;
            if (otherAddr != null) {
              otherAddr = otherAddr.trim();
            }
            if (otherAddr == null || otherAddr.length() == 0) {
              otherAddr = locId.getHost().getHostName();
              otherAddr2 = locId.getHost().getCanonicalHostName(); // some people use fqns
            }
            if ( !( (
                     otherAddr.equals(myAddr) || (otherAddr2 != null && otherAddr2.equals(myAddr)) || (myAddr2 != null  && myAddr2.equals(otherAddr2))
                     )
                   && locId.getPort() == this.port) ) {
              log.getLogWriter().info(ExternalStrings.GossipServer_0__1__ATTEMPTING_TO_GET_STATE_FROM__2__3_, new Object[] {myAddr, Integer.valueOf(this.port), otherAddr, Integer.valueOf(locId.getPort())});
              if (recover(otherAddr, locId.getPort())) {
                return true;
              }
            }
          }
        } // while()
      } // if (locatorsString)

      return false;
    }
    
    /** try to recover gossip state from another locator */
    private boolean recover(String address, int serverPort) {

      // Contact existing locator first to find out all members in system.
      GossipClient gc = new GossipClient(
          new IpAddress(address, serverPort), 20000);
      List info = gc.getMembers(CHANNEL_NAME, (Address)null, false, 15000);

      if (gc.getResponsiveServerCount() > 0) {
        List mbrs = new VersionedEntryList(info.size());
        for (int i=0; i<info.size(); i++) {
          mbrs.add(new LocatorEntry((Address)info.get(i)));
        }
        // GemStoneAddition (comment)
        // Note that CacheCleaner has not yet started, so it is
        // not necessary to synchronize this update.
        groups.put(CHANNEL_NAME, mbrs);
        log.getLogWriter().info(ExternalStrings.GossipServer_INITIAL_DISCOVERY_SET_IS_0,
            mbrs);
        processLocators(log, locators, gc.getGossip_servers());
        return true;
      }
      return false;
    }
    
    /** recover gossip state from the given file
     * 
     * @param sFile the file containing gossip state from a previous run
     * @return true if state was recovered from disk 
     */
    private boolean recoverFromFile(File sFile) {
      if (sFile.exists()) {
        log.getLogWriter().info(ExternalStrings.GossipServer_RECOVERING_STATE_FROM__0, sFile);
        FileInputStream fis = null;
        try {
          VersionedEntryList members = null;
          fis = new FileInputStream(sFile);
          ObjectInput ois = new ObjectInputStream(fis);
          try {
            int fileversion = ois.readInt();
            log.info("Discovery set was written with file version " + fileversion);
            if (fileversion <= FILE_FORMAT_VERSION) {
              short gfVersion = (short)(FILE_FORMAT_TO_GEMFIRE_VERSION_MAP.get(fileversion).intValue());
              Version gemFireVersion = Version.fromOrdinalNoThrow(gfVersion, false);
              if ( gemFireVersion != null) {
                log.getLogWriter().info(ExternalStrings.DEBUG, "Discovery set was written with " + gemFireVersion);
                ois = new VersionedObjectInput(ois, gemFireVersion);
              } else {
                return false;
              }
            } else {
              return false;
            }
            InetAddress addr = (InetAddress)ois.readObject();
            if (addr != null && this.bind_address != null && !addr.equals(this.bind_address)) {
              return false;
            }
            members = new VersionedEntryList();
            members.readExternal(ois);
          }
          finally {
            ois.close();
          }
          // GemStoneAddition (comment)
          // Note that CacheCleaner has not started yet, so it
          // is not necessary to synchronize this update to groups.
          groups.put(CHANNEL_NAME, members);
          log.getLogWriter().info(ExternalStrings.GossipServer_INITIAL_DISCOVERY_SET_IS_0,
              members);
          return true;
        }
        catch (Exception e) {
          e.printStackTrace();
          log.getLogWriter().warning(ExternalStrings.GossipServer_UNABLE_TO_RECOVER_LOCATOR_REGISTRY_FROM__0, sFile, e);
          
          // GemStoneAddition: close the stream before attempting to delete the stateFile,
          // otherwise intelligent file systems (like Windows) will not delete the file.
          if (fis != null) {
            try { fis.close(); } catch (IOException e2) { }
          }
          
          // GemStoneAddition: check return of delete()
          if (!sFile.delete() && sFile.exists()) {
            log.getLogWriter().warning(ExternalStrings.GossipServer_UNABLE_TO_DELETE_REGISTRY_FILE_DISABLING_REGISTRY_PERSISTENCE);
            this.stateFile = null;
          }
        }
        finally {
          // GemStoneAddition make sure the streams are closed
          if (fis != null) {
            try { fis.close(); } catch (IOException e) { }
          }
        }
      }
      return false;
    }

    /**
     * @param daddress the address of the distributed system
     */
    public void setLocalAddress(InternalDistributedMember daddress) {
      Address jgroupsAddress = ((JGroupMember)daddress.getNetMember()).getAddress();
      setLocalAddress(jgroupsAddress);
    }

    public void init(TcpServer server) {
      super.init();
      if (log.getLogWriter().fineEnabled()) {
        this.log.getLogWriter().fine("Recovering from state file: " + stateFile);
      }
      this.timer = new Timer(true);
      recover(stateFile, locatorString);
      cache_cleaner=new CacheCleaner();
      timer.schedule(cache_cleaner, expiry_time, expiry_time);
      this.stateFile = stateFile;
    }

    public GossipServer.EntryList newEntryList() {
      return new VersionedEntryList();
    }
    
    public Entry newEntry(Address mbr) {
      return new LocatorEntry(mbr);
    }

    public void persistState() {
      if (stateFile != null) {
        VersionedEntryList members;
        synchronized (groups) { // GemStoneAddition
          members = (VersionedEntryList)groups.get(CHANNEL_NAME);
          if (members == null) {
            members = new VersionedEntryList();
            groups.put(CHANNEL_NAME, members);
          }
        }
        if (!stateFile.delete() && stateFile.exists()) {
          log.getLogWriter().warning(
            ExternalStrings.GossipServer_UNABLE_TO_DELETE_0, stateFile.getAbsolutePath());
        }
        try {
          FileOutputStream fos = null;
          ObjectOutputStream oos = null;
          VersionedObjectOutput vos = null;
          try { // GemStoneAddition assure stream closure
            fos = new FileOutputStream(stateFile);
            oos = new ObjectOutputStream(fos);
            Version v = Version.fromOrdinal((short)
                ((Integer)FILE_FORMAT_TO_GEMFIRE_VERSION_MAP.get(FILE_FORMAT_VERSION)).intValue(),
                false);
            vos = new VersionedObjectOutput(oos, v);
            vos.writeInt(FILE_FORMAT_VERSION);
            vos.writeObject(this.bind_address);
            synchronized (members) { // GemStoneAddition members needs to be consistent
              log.info("persisting members: " + members);
              members.writeExternal(vos);
            }
            vos.flush();
            oos.flush();
            fos.flush();
          }
          finally {
            if (vos != null) {
              vos.close();
            }

            if (oos != null) {
              oos.close();
            }
            // In case the FileOutputStream got created but the ObjectOutputStream failed...
            if (fos != null) {
              fos.close();
            }
          }
        }
        catch (Exception e) {
          log.getLogWriter().warning(ExternalStrings.GossipServer_ERROR_WRITING_LOCATOR_STATE_TO_DISK__STATE_STORAGE_IS_BEING_DISABLED, e);
          this.stateFile = null;
        }
      }
    }


    /**
     * note that it is okay for this to return false if this locator will have a
     * DS but it has not yet been created
     * @return true if there is a distributed system running in this vm
     */
    public boolean hasDistributedSystem() {
      return withDS || InternalDistributedSystem.unsafeGetConnectedInstance() != null;
    }
    

    /* -------------------------------- End of Private methods ----------------------------------- */

    /**
     *   Maintains the member address plus a timestamp. Used by CacheCleaner thread to remove old entries.
     */
    public static class LocatorEntry extends GossipServer.Entry implements Externalizable {
        LocatorEntry(Address mbr) {
          super(mbr);
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
    private static class VersionedEntryList extends GossipServer.EntryList implements Externalizable {

      // For testing only
      private int TEST_BYTES = 1;

      public VersionedEntryList(int size) {
        super(size);
      }

      public VersionedEntryList() {
        super();
      }

      @Override
      public void readExternal(ObjectInput in) throws IOException,
          ClassNotFoundException {

        // File format version from GF7.0.X to 7.1.X has been changed.
        if (TcpServer.isTesting && in instanceof VersionedObjectInput) {
          VersionedObjectInput vis = (VersionedObjectInput)in;
          short gfVersion = vis.getVersion().ordinal();
          int newFileVersion = getFileVersionForOrdinal(Version.GFE_71.ordinal());
          int oldFileVersion = getFileVersionForOrdinal(gfVersion);
          if (oldFileVersion < newFileVersion) {
            if(in.readInt() != TEST_BYTES){
              throw new IOException("Read integer is not TEST_BYTES ");
            }
          }
        }
        
        int size = in.readInt();
        for (int i=0; i<size; i++) {
          LocatorEntry entry = new LocatorEntry(null);
          entry.readExternal(in);
          add(entry);
        }
      }

      @Override
      public void writeExternal(ObjectOutput out) throws IOException {

     // For test purpose only. File format version from GF7.0.X to 7.1.X has been changed.
        if (TcpServer.isTesting && out instanceof VersionedObjectOutput) {
          VersionedObjectOutput vos = (VersionedObjectOutput)out;
          short gfVersion = vos.getVersion().ordinal();
          int newFileVersion = getFileVersionForOrdinal(Version.GFE_71.ordinal());
          int oldFileVersion = getFileVersionForOrdinal(gfVersion);
          if (oldFileVersion < newFileVersion) {
            out.writeInt(TEST_BYTES);
          }
        }

        out.writeInt(this.size());
        Iterator<Entry> itr = this.iterator();
        while(itr.hasNext()) {
          Entry entry = itr.next();
          entry.writeExternal(out);
        }
      }
    }

  /**
   * Returns File Version for older Gemfire versions. If we have mappings
   * "[1000 -> GF_70], [1001 -> GF_71]" and passed version is GF_701 and there
   * is no mapping for GF_701 we will return 1000 which is lower closest
   * possible to GF_701.
   * 
   * @param ordinal
   * @return gossip version
   */
    public static int getFileVersionForOrdinal(short ordinal) {

      // Sanity check
      short closest = -1;
      int closestFV = LocatorImpl.FILE_FORMAT_VERSION;
      if (ordinal <= Version.CURRENT_ORDINAL) {
        Iterator<Map.Entry<Integer,Integer>> itr = LocatorImpl.FILE_FORMAT_TO_GEMFIRE_VERSION_MAP.entrySet().iterator();
        while (itr.hasNext()) {
          Map.Entry<Integer,Integer> entry = itr.next();
          short o = (short)entry.getValue().intValue();
          if (o == ordinal) {
            return ((Integer)entry.getKey()).intValue();
          } else if (o < ordinal && o > closest ) {
            closest = o;
            closestFV = ((Integer)entry.getKey()).intValue();
          }
        }
      }

      return closestFV;
    }

    public static Map getFileVersionMapForTestOnly() {
      return FILE_FORMAT_TO_GEMFIRE_VERSION_MAP;
    }

}
