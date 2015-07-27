package com.gemstone.gemfire.distributed.internal.membership.gms.locator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Locator;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpHandler;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

public class GMSLocator implements TcpHandler, Locator {

  private static final int LOCATOR_FILE_STAMP = 0x7b8cf741;
  
  private static final Logger logger = LogService.getLogger();

  private final int port;
  private final InetAddress bindAddress;
  private final boolean usePreferredCoordinators;
  private final boolean networkPartitionDetectionEnabled;
  private final String locatorString;
  private final List<InetSocketAddress> locators;
  private Services services;
  
  private Set<InternalDistributedMember> registrants = new HashSet<InternalDistributedMember>();

  private NetView view;

  private File viewFile;

  /**
   * @param mgr               the membership manager
   * @param port              the tcp/ip server socket port number
   * @param bindAddress      network address to bind to
   * @param stateFile         the file to persist state to/recover from
   * @param locatorString     location of other locators (bootstrapping, failover)
   * @param usePreferredCoordinators    true if the membership coordinator should be a Locator
   * @param networkPartitionDetectionEnabled true if network partition detection is enabled
   */
  public GMSLocator(int port,
                      InetAddress bindAddress,
                      File stateFile,
                      String locatorString,
                      boolean usePreferredCoordinators,
                      boolean networkPartitionDetectionEnabled) {
    this.port = port;
    this.bindAddress = bindAddress;
    this.usePreferredCoordinators = usePreferredCoordinators;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.locatorString = locatorString;
    if (this.locatorString == null || this.locatorString.length() == 0) {
      this.locators = new ArrayList<InetSocketAddress>(0);
    } else {
      this.locators = GMSUtil.parseLocators(locatorString, bindAddress);
    }
    this.viewFile = stateFile;
  }
  
  /**
   * This must be called after booting the membership manager so
   * that the locator can use its services
   * @param svc
   */
  public void setMembershipManager(MembershipManager mgr) {
    logger.info("Peer locator is connecting to local membership services");
    services = ((GMSMembershipManager)mgr).getServices();
    services.setLocator(this);
    this.view = services.getJoinLeave().getView();
  }
  
  public void init(TcpServer server) {
    recover();
  }
  
  private void findServices() {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && services == null) {
      logger.info("Peer locator found distributed system " + sys);
      setMembershipManager(sys.getDM().getMembershipManager());
    }
  }

  public void installView(NetView view) {
    synchronized(this.registrants) {
      registrants.clear();
    }
    logger.info("Peer locator received new membership view: " + view);
    this.view = view;
    saveView(view);
  }
  
  
  @Override
  public Object processRequest(Object request) throws IOException {
    Object response = null;
    
    if (logger.isDebugEnabled()) {
      logger.debug("Peer locator processing " + request);
    }
    
    if (request instanceof GetViewRequest) {
      if (view != null) {
        response = new GetViewResponse(view);
      }
    } else if (request instanceof FindCoordinatorRequest) {
      FindCoordinatorRequest findRequest = (FindCoordinatorRequest)request;
      
      if (findRequest.getMemberID() != null) {
        InternalDistributedMember coord = null;

        // at this level we want to return the coordinator known to membership services,
        // which may be more up-to-date than the one known by the membership manager
        if (view == null) {
          findServices();
        }
        if (view != null) {
          coord = view.getCoordinator();
        }
        
        if (coord != null) {
          // no need to keep track of registrants after we're in the distributed system
          synchronized(registrants) {
            registrants.clear();
          }
          
        } else {
          // find the "oldest" registrant
          synchronized(registrants) {
            registrants.add(findRequest.getMemberID());
            if (services != null) {
              coord = services.getJoinLeave().getMemberID();
            }
            for (InternalDistributedMember mbr: registrants) {
              if (mbr != coord  &&  (coord==null  ||  mbr.compareTo(coord) < 0)) {
                coord = mbr;
              }
            }
          }
        }
        response = new FindCoordinatorResponse(coord,
            this.networkPartitionDetectionEnabled, this.usePreferredCoordinators);
      }
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Peer locator returning " + response);
    }
    return response;
  }

  public void saveView(NetView view) {
    if (viewFile == null) {
      return;
    }
    if (!viewFile.delete() && viewFile.exists()) {
      logger.warn("Peer locator is unable to delete persistent membership information in " +
          viewFile.getAbsolutePath());
    }
    try {
      ObjectOutputStream oos = null;
      try {
        oos = new ObjectOutputStream(new FileOutputStream(viewFile));
        oos.writeInt(LOCATOR_FILE_STAMP);
        oos.writeInt(Version.CURRENT_ORDINAL);
        DataSerializer.writeObject(view, oos);
      }
      finally {
        oos.flush();
        oos.close();
      }
    }
    catch (Exception e) {
      logger.warn("Peer locator encountered an error writing current membership to disk.  Disabling persistence.  Care should be taken when bouncing this locator as it will not be able to recover knowledge of the running distributed system", e);
      this.viewFile = null;
      // TODO - force TcpServer to shut down if this happens instead of continuing to run
    }
  }

  
  @Override
  public void endRequest(Object request, long startTime) {
    // place holder for statistics
  }

  @Override
  public void endResponse(Object request, long startTime) {
    // place holder for statistics
  }

  @Override
  public void shutDown() {
  }
  
  
  // test hook
  public List<InternalDistributedMember> getMembers() {
    if (view != null) {
      return new ArrayList<InternalDistributedMember>(view.getMembers());
    } else {
      synchronized(registrants) {
        return new ArrayList<InternalDistributedMember>(registrants);
      }
    }
  }

  @Override
  public void restarting(DistributedSystem ds, GemFireCache cache,
      SharedConfiguration sharedConfig) {
    setMembershipManager(((InternalDistributedSystem)ds).getDM().getMembershipManager());
  }

  public void recover() {
    if (!recoverFromOthers()) {
      recoverFromFile(viewFile);
    }
  }
  
  private boolean recoverFromOthers() {
    if (locators.isEmpty()) {
      return false;
    }

    for (InetSocketAddress other: this.locators) {
      logger.info("Peer locator attempting to get state from " + other);
      if (recover(other)) {
        return true;
      }
    } // for
    return false;
  }
  
  private boolean recover(InetSocketAddress other) {
    try {
      Object response = TcpClient.requestToServer(other.getAddress(), other.getPort(),
          new GetViewRequest(), 20000, true);
      if (response != null && (response instanceof GetViewResponse)) {
        this.view = ((GetViewResponse)response).getView();
        logger.info("Peer locator recovered initial membership of {}", view);
        return true;
      }
    } catch (IOException e) {
      // ignore
    } catch (ClassNotFoundException e) {
      // hmm - odd response?
    }
    return false;
  }
  
  private boolean recoverFromFile(File file) {
    if (!file.exists()) {
      return false;
    }
    logger.info("Peer locator recovering from " + file.getAbsolutePath());
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      ObjectInput ois = new ObjectInputStream(fis);
      
      int magic = ois.readInt();
      if (magic != LOCATOR_FILE_STAMP) {
        return false;
      }
      
      int version = ois.readInt();
      Version geodeVersion = Version.fromOrdinalNoThrow((short)version, false);
      if (geodeVersion != null  &&  version != Version.CURRENT_ORDINAL) {
        logger.info("Peer locator found that persistent view was written with {}", geodeVersion);
        ois = new VersionedObjectInput(ois, geodeVersion);
      } else {
        return false;
      }

      this.view = DataSerializer.readObject(ois);

      logger.info("Initial membership is " + view);
      return true;
    }
    catch (Exception e) {
      e.printStackTrace();
      logger.warn(LocalizedStrings.Locator_unable_to_recover_view_0
          .toLocalizedString(file), e);
      try {
        fis.close();
      } catch (IOException ex) {
        // ignore
      }
      if (!file.delete() && file.exists()) {
        logger.warn("Peer locator was unable to recover from or delete " + file);
        this.viewFile = null;
      }
    }
    finally {
      try { fis.close(); } catch (IOException e) { }
    }
    return false;
  }

}
