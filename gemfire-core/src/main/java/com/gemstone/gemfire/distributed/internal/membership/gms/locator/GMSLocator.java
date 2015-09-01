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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.gemstone.gemfire.InternalGemFireException;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.LocatorStats;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember;
import com.gemstone.gemfire.distributed.internal.membership.MembershipManager;
import com.gemstone.gemfire.distributed.internal.membership.NetView;
import com.gemstone.gemfire.distributed.internal.membership.gms.GMSUtil;
import com.gemstone.gemfire.distributed.internal.membership.gms.NetLocator;
import com.gemstone.gemfire.distributed.internal.membership.gms.Services;
import com.gemstone.gemfire.distributed.internal.membership.gms.interfaces.Locator;
import com.gemstone.gemfire.distributed.internal.membership.gms.mgr.GMSMembershipManager;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpClient;
import com.gemstone.gemfire.distributed.internal.tcpserver.TcpServer;
import com.gemstone.gemfire.internal.Version;
import com.gemstone.gemfire.internal.VersionedObjectInput;
import com.gemstone.gemfire.internal.logging.LogService;

import static com.gemstone.gemfire.internal.i18n.LocalizedStrings.LOCATOR_UNABLE_TO_RECOVER_VIEW;

public class GMSLocator implements Locator, NetLocator {

  /* package */ static final int LOCATOR_FILE_STAMP = 0x7b8cf741;
  
  private static final Logger logger = LogService.getLogger();

  private final boolean usePreferredCoordinators;
  private final boolean networkPartitionDetectionEnabled;
  private final String locatorString;
  private final List<InetSocketAddress> locators;
  private Services services;
  private final LocatorStats stats;
  
  private Set<InternalDistributedMember> registrants = new HashSet<InternalDistributedMember>();

  private NetView view;

  private File viewFile;

  /**
   * @param bindAddress       network address that TcpServer will bind to
   * @param stateFile         the file to persist state to/recover from
   * @param locatorString     location of other locators (bootstrapping, failover)
   * @param usePreferredCoordinators    true if the membership coordinator should be a Locator
   * @param networkPartitionDetectionEnabled true if network partition detection is enabled
   * @param stats the locator statistics object
   */
  public GMSLocator(  InetAddress bindAddress,
                      File stateFile,
                      String locatorString,
                      boolean usePreferredCoordinators,
                      boolean networkPartitionDetectionEnabled, LocatorStats stats) {
    this.usePreferredCoordinators = usePreferredCoordinators;
    this.networkPartitionDetectionEnabled = networkPartitionDetectionEnabled;
    this.locatorString = locatorString;
    if (this.locatorString == null || this.locatorString.length() == 0) {
      this.locators = new ArrayList<InetSocketAddress>(0);
    } else {
      this.locators = GMSUtil.parseLocators(locatorString, bindAddress);
    }
    this.viewFile = stateFile;
    this.stats = stats;
  }
  
  @Override
  public void setMembershipManager(MembershipManager mgr) {
    if (services == null) {
      logger.info("Peer locator is connecting to local membership services");
      services = ((GMSMembershipManager)mgr).getServices();
      services.setLocator(this);
      NetView newView = services.getJoinLeave().getView();
      if (newView != null) {
        this.view = newView;
      }
    }
  }
  
  @Override
  public void init(TcpServer server) throws InternalGemFireException {
    recover();
  }
  
  private void findServices() {
    InternalDistributedSystem sys = InternalDistributedSystem.getAnyInstance();
    if (sys != null && services == null) {
      logger.info("Peer locator found distributed system " + sys);
      setMembershipManager(sys.getDM().getMembershipManager());
    }
  }

  @Override
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
    
    if (logger.isInfoEnabled()) {
      logger.info("Peer locator processing {}", request);
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
        
        boolean fromView = false;
        
        if (view != null) {
          coord = view.getCoordinator(findRequest.getRejectedCoordinators());
          logger.info("Peer locator: coordinator from view is {}", coord);
          fromView = true;
        }
        
        if (coord != null) {
          // no need to keep track of registrants after we're in the distributed system
          synchronized(registrants) {
            registrants.clear();
          }
          
        } else {
          // find the "oldest" registrant
          Collection<InternalDistributedMember> rejections = findRequest.getRejectedCoordinators();
          if (rejections == null) {
            rejections = Collections.emptyList();
          }
          synchronized(registrants) {
            registrants.add(findRequest.getMemberID());
            if (services != null) {
              coord = services.getJoinLeave().getMemberID();
            }
            for (InternalDistributedMember mbr: registrants) {
              if (mbr != coord  &&  (coord==null  ||  mbr.compareTo(coord) < 0)) {
                if (!rejections.contains(mbr)
                    && (mbr.getNetMember().preferredForCoordinator() || !mbr.getNetMember().splitBrainEnabled())) {
                  coord = mbr;
                }
              }
            }
            logger.info("Peer locator: coordinator from registrations is {}", coord);
          }
        }
        response = new FindCoordinatorResponse(coord, fromView,
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
    stats.endLocatorRequest(startTime);
  }

  @Override
  public void endResponse(Object request, long startTime) {
    stats.endLocatorResponse(startTime);
  }

  @Override
  public void shutDown() {
    // nothing to do for GMSLocator
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

  public void recover() throws InternalGemFireException {
    if (!recoverFromOthers()) {
      recoverFromFile(viewFile);
    }
  }
  
  private boolean recoverFromOthers() {
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
    } catch (IOException | ClassNotFoundException ignore) {
      logger.info("Peer locator could not recover membership view from {}: {}", other, ignore.getMessage());
    }
    return false;
  }

  /* package */ boolean recoverFromFile(File file) throws InternalGemFireException {
    if (!file.exists()) {
      return false;
    }

    logger.info("Peer locator recovering from " + file.getAbsolutePath());
    try (ObjectInput ois = new ObjectInputStream(new FileInputStream(file))) {
      if (ois.readInt() != LOCATOR_FILE_STAMP) {
        return false;
      }

      ObjectInput ois2 = ois;
      int version = ois2.readInt();
      if (version != Version.CURRENT_ORDINAL) {
        Version geodeVersion = Version.fromOrdinalNoThrow((short)version, false);
        logger.info("Peer locator found that persistent view was written with {}", geodeVersion);
        ois2 = new VersionedObjectInput(ois2, geodeVersion);
      }
    
      this.view = DataSerializer.readObject(ois2);
      logger.info("Peer locator initial membership is " + view);
      return true;

    } catch (Exception e) {
      String msg = LOCATOR_UNABLE_TO_RECOVER_VIEW.toLocalizedString(file.toString());
      logger.warn(msg, e);
      if (!file.delete() && file.exists()) {
        logger.warn("Peer locator was unable to recover from or delete " + file);
        this.viewFile = null;
      }
      throw new InternalGemFireException(msg, e);
    }
  }

}
