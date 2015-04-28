/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: GMS.java,v 1.49 2005/12/23 14:57:06 belaban Exp $

package com.gemstone.org.jgroups.protocols.pbcast;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.Event;
import com.gemstone.org.jgroups.Global;
import com.gemstone.org.jgroups.Header;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.Membership;
import com.gemstone.org.jgroups.MergeView;
import com.gemstone.org.jgroups.Message;
import com.gemstone.org.jgroups.ShunnedAddressException;
import com.gemstone.org.jgroups.SuspectMember;
import com.gemstone.org.jgroups.TimeoutException;
import com.gemstone.org.jgroups.View;
import com.gemstone.org.jgroups.ViewId;
import com.gemstone.org.jgroups.protocols.FD_SOCK;
import com.gemstone.org.jgroups.protocols.UNICAST;
import com.gemstone.org.jgroups.stack.GossipServer;
import com.gemstone.org.jgroups.stack.IpAddress;
import com.gemstone.org.jgroups.stack.Protocol;
import com.gemstone.org.jgroups.util.AckCollector;
import com.gemstone.org.jgroups.util.BoundedList;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.util.GFStringIdImpl;
import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.Promise;
import com.gemstone.org.jgroups.util.QueueClosedException;
import com.gemstone.org.jgroups.util.Streamable;
import com.gemstone.org.jgroups.util.TimeScheduler;
import com.gemstone.org.jgroups.util.Util;

/**
 * Group membership protocol. Handles joins/leaves/crashes (suspicions) and emits new views
 * accordingly. Use VIEW_ENFORCER on top of this layer to make sure new members don't receive
 * any messages until they are members.
 */
public class GMS extends Protocol  {
    protected/*GemStoneAddition*/ GmsImpl           impl=null;
    Address                   local_addr=null;
    final Membership          members=new Membership();     // real membership
    private final Membership  tmp_members=new Membership(); // base for computing next view
    static final String       FAILED_TO_ACK_MEMBER_VERIFY_THREAD = "Failed to ACK member verify Thread" ;

    /** Members joined but for which no view has been received yet */
    private final Vector      joining=new Vector(7);

    /** Members excluded from group, but for which no view has been received yet */
    private final Vector      leaving=new Vector(7);

    View                      view=null;
    ViewId                    view_id=null;
    private long              ltime=0;
    long                      join_timeout=5000;
    long                      join_retry_timeout=2000;
    long                      leave_timeout=5000;
    private long              digest_timeout=0;              // time to wait for a digest (from PBCAST). should be fast
    long                      merge_timeout=10000;           // time to wait for all MERGE_RSPS
    private final Object      impl_mutex=new Object();       // synchronizes event entry into impl
    private final Object      digest_mutex=new Object();
    private final Promise     digest_promise=new Promise();  // holds result of GET_DIGEST event
    private final Hashtable   impls=new Hashtable(3);
    private boolean           shun=true;
    boolean                   merge_leader=false;         // can I initiate a merge ?
    private boolean           print_local_addr=true;
    boolean                   disable_initial_coord=false; // can the member become a coord on startup or not ?
    /** Setting this to false disables concurrent startups. This is only used by unit testing code
     * for testing merging. To everybody else: don't change it to false ! */
    boolean                   handle_concurrent_startup=true;
    static final String       CLIENT="Client";
    static final String       COORD="Coordinator";
    static final String       PART="Participant";
    TimeScheduler             timer=null;
    private volatile boolean joined; // GemStoneAddition - has this member joined?
    private volatile boolean disconnected; // GemStoneAddition - has this member disconnected?
    /**
     * GemStoneAddition - network partition detection uses a "leader"
     * process to decide which partition will survive.
     */
    volatile Address leader;
    
    /**
     * GemStoneAddition - are coordinators forced to be colocated with gossip servers?
     */
    boolean floatingCoordinatorDisabled;
    
    /**
     * GemStoneAddition - is this process split-brain-detection-enabled?  If so,
     * it's eligible to be the leader process.
     */
    boolean splitBrainDetectionEnabled;
    
    /**
     * GemStoneAddition - indicates whether a network partition has already been detected.
     * This is used to prevent this member from installing a new view after a network partition.
     * See bug #39279
     */
    protected boolean networkPartitionDetected = false;
    
    /** Max number of old members to keep in history */
    protected int             num_prev_mbrs=50;

    /** Keeps track of old members (up to num_prev_mbrs) */
    BoundedList               prev_members=null;

    int num_views=0;

    /** Stores the last 20 views */
    BoundedList               prev_views=new BoundedList(20);


    /** Class to process JOIN, LEAVE and MERGE requests */
    public final ViewHandler  view_handler=new ViewHandler();

    /** To collect VIEW_ACKs from all members */
    final AckCollector ack_collector=new AckCollector();
    
    /** To collect PREPARE_FOR_VIEW acks from members */
    final AckCollector prepare_collector = new AckCollector(); // GemStoneAddition

    /** Time in ms to wait for all VIEW acks (0 == wait forever) */
    long                      view_ack_collection_timeout=12437;  // GemStoneAddition - was 20000;
                                                       // for 6.5 release, changed from 17437 to 12437 - well below the default join timeout period

    /** How long should a Resumer wait until resuming the ViewHandler */
    long                      resume_task_timeout=17439; // GemStoneAddition - was 20000;

    private int partitionThreshold = 51; // GemStoneAddition
    
    private int memberWeight; // GemStoneAddition - see network partition detection spec
    private volatile View preparedView; // GemStoneAddition - see network partition detection spec
    private View previousPreparedView; // GemStoneAddition
    private volatile Address coordinator;

    public static final String       name="GMS";
    
    /** GemStoneAddition - if set this causes view casting to be delayed by some number of seconds */
    public static int TEST_HOOK_SLOW_VIEW_CASTING;
    
    /**
     * GemStoneAddition - amount of time to wait for additional join/leave
     * requests before processing.  Set gemfire.VIEW_BUNDLING_WAIT_TIME to
     * the number of milliseconds.  Defaults to 150ms.
     */
    static final long BUNDLE_WAITTIME = Integer.getInteger("gemfire.VIEW_BUNDLING_WAIT_TIME", 150).intValue();

    private Object installViewLock = new Object(); //lock to assure that install views will atomically update the view, transition coordinators and notify locators

    public GMS() {
        initState();
    }


    @Override // GemStoneAddition  
    public String getName() {
        return name;
    }
    
    // start GemStoneAddition
    @Override // GemStoneAddition  
    public int getProtocolEnum() {
      return com.gemstone.org.jgroups.stack.Protocol.enumGMS;
    }
    // end GemStone addition



    public String getView() {return view_id != null? view_id.toString() : "null";}
    public int getNumberOfViews() {return num_views;}
    public String getLocalAddress() {return local_addr != null? local_addr.toString() : "null";}
    public String getMembers() {return members != null? members.toString() : "[]";}
    public int getNumMembers() {return members != null? members.size() : 0;}
    public long getJoinTimeout() {return join_timeout;}
    public void setJoinTimeout(long t) {join_timeout=t;}
    public long getJoinRetryTimeout() {return join_retry_timeout;}
    public void setJoinRetryTimeout(long t) {join_retry_timeout=t;}
    public boolean isShun() {return shun;}
    public void setShun(boolean s) {shun=s;}
    public String printPreviousMembers() {
        StringBuffer sb=new StringBuffer();
        if(prev_members != null) {
            for(Enumeration en=prev_members.elements(); en.hasMoreElements();) {
                sb.append(en.nextElement()).append("\n");
            }
        }
        return sb.toString();
    }

    public int viewHandlerSize() {return view_handler.size();}
    public boolean isViewHandlerSuspended() {return view_handler.suspended();}
    public String dumpViewHandlerQueue() {
        return view_handler.dumpQueue();
    }
    public String dumpViewHandlerHistory() {
        return view_handler.dumpHistory();
    }
    public void suspendViewHandler() {
        view_handler.suspend(null);
    }
    public void resumeViewHandler() {
        view_handler.resumeForce();
    }

    GemFireTracer getLog() {return log;}

    public String printPreviousViews() {
        StringBuffer sb=new StringBuffer();
        for(Enumeration en=prev_views.elements(); en.hasMoreElements();) {
            sb.append(en.nextElement()).append("\n");
        }
        return sb.toString();
    }

    public boolean isCoordinator() {
        Address coord=determineCoordinator();
        return coord != null && local_addr != null && local_addr.equals(coord);
    }
    
    /**
     * For testing we sometimes need to be able to disable disconnecting during
     * failure scenarios
     */
    public void disableDisconnectOnQuorumLossForTesting() {
      this.splitBrainDetectionEnabled = false;
    }


    @Override // GemStoneAddition  
    public void resetStats() {
        super.resetStats();
        num_views=0;
        prev_views.removeAll();
    }


    @Override // GemStoneAddition  
    public Vector requiredDownServices() {
        Vector retval=new Vector(3);
        retval.addElement(Integer.valueOf(Event.GET_DIGEST));
        retval.addElement(Integer.valueOf(Event.SET_DIGEST));
        retval.addElement(Integer.valueOf(Event.FIND_INITIAL_MBRS));
        return retval;
    }


    public void setImpl(GmsImpl new_impl) {
        synchronized(impl_mutex) {
            if(impl == new_impl) // superfluous
                return;
            impl=new_impl;
//            if (impl instanceof CoordGmsImpl) {
//              log.getLogWriter().config(
//                JGroupsStrings.GMS_THIS_MEMBER_IS_BECOMING_GROUP_COORDINATOR);
//            }
            if(log.isDebugEnabled()) {
                String msg=(local_addr != null? local_addr.toString()+" " : "") + "changed role to " + new_impl.getClass().getName();
                log.debug(msg);
            }
        }
    }


    public GmsImpl getImpl() {
        return impl;
    }


    @Override // GemStoneAddition  
    public void init() throws Exception {
        prev_members=new BoundedList(num_prev_mbrs);
        timer=stack != null? stack.timer : null;
        if(timer == null)
            throw new Exception("GMS.init(): timer is null");
        if(impl != null)
            impl.init();
    }

    @Override // GemStoneAddition  
    public void start() throws Exception {
      this.disconnected = false;
        if(impl != null) impl.start();
    }

    @Override // GemStoneAddition  
    public void stop() {
        view_handler.stop(true);
        if(impl != null) impl.stop();
        if(prev_members != null)
            prev_members.removeAll();
    }


    // GemStoneAddition - added suspects argument for bug 41772
    public synchronized void becomeCoordinator(Vector suspects) {
      if (!(impl instanceof CoordGmsImpl)) { // GemStoneAddition, synchronize and checked for redundant becomeCoordinator
        log.getLogWriter().info(ExternalStrings.GMS_THIS_MEMBER_0_IS_BECOMING_GROUP_COORDINATOR, this.local_addr/*, new Exception("stack trace")*/);
        CoordGmsImpl tmp=(CoordGmsImpl)impls.get(COORD);
        if(tmp == null) {
            tmp=new CoordGmsImpl(this);
            impls.put(COORD, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error(ExternalStrings.GMS_EXCEPTION_SWITCHING_TO_COORDINATOR_ROLE, e);
        }
        if (((IpAddress)this.local_addr).getBirthViewId() < 0) {
          ((IpAddress)this.local_addr).setBirthViewId(
              this.view_id == null? 0 : this.view_id.getId());
        }
        setImpl(tmp);
        if (suspects != null && suspects.size() > 0) {
          List suspectList = new LinkedList(suspects);
          impl.handleLeave(suspectList, true,
              Collections.singletonList("Member was suspected of being dead prior to " + this.local_addr
              + " becoming group coordinator"), false);
        }
      }
    }
    
    // GemStoneAddition - logical time can become confused when a
    // coordinator sends out a view using unicast and the view isn't
    // sent all members before the coordinator dies.  We increment the
    // Lamport clock to avoid missing updates
    protected synchronized void incrementLtime(int amount) {
      ltime += amount;
    }


    public void becomeParticipant() {
      if (this.stack.getChannel().closing()) { // GemStoneAddition - fix for bug #42969
        return; // don't try to become a participant if we're shutting down
      }
        ParticipantGmsImpl tmp=(ParticipantGmsImpl)impls.get(PART);

        if(tmp == null) {
            tmp=new ParticipantGmsImpl(this);
            impls.put(PART, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error(ExternalStrings.GMS_EXCEPTION_SWITCHING_TO_PARTICIPANT, e);
        }
        setImpl(tmp);
    }

    public void becomeClient() {
        ClientGmsImpl tmp=(ClientGmsImpl)impls.get(CLIENT);
        if(tmp == null) {
            tmp=new ClientGmsImpl(this);
            impls.put(CLIENT, tmp);
        }
        try {
            tmp.init();
        }
        catch(Exception e) {
            log.error(ExternalStrings.GMS_EXCEPTION_SWITCHING_TO_CLIENT_ROLE, e);
        }
        setImpl(tmp);
    }


    boolean haveCoordinatorRole() {
        return impl != null && impl instanceof CoordGmsImpl;
    }
    
    boolean haveParticipantRole() {
      return impl != null && impl instanceof ParticipantGmsImpl;
    }


    /**
     * Computes the next view. Returns a copy that has <code>old_mbrs</code> and
     * <code>suspected_mbrs</code> removed and <code>new_mbrs</code> added.
     */
    public View getNextView(Vector added_mbrs, Vector left_mbrs, Vector suspected_mbrs) {
        Vector mbrs;
        long vid;
        View v;
        Membership tmp_mbrs;
        Address tmp_mbr;

        synchronized(members) {
            if(view_id == null) {
//                return null; // this should *never* happen ! GemStoneAddition
//                log.error("view_id is null", new Exception()); // GemStoneAddition debug
                vid = 0; // GemStoneAddition
            }
            else {
              vid=Math.max(view_id.getId(), ltime) + 1;
            }
            ltime=vid;
            tmp_mbrs=tmp_members.copy();  // always operate on the temporary membership
            
            if (suspected_mbrs != null) { // GemStoneAddition - if a mbr is shutting down, just remove it
              if (left_mbrs == null) {
                left_mbrs = new Vector();
              }
              for (Iterator<IpAddress> it = suspected_mbrs.iterator(); it.hasNext(); ) {
                IpAddress addr = it.next();
                if (this.stack.gfPeerFunctions.isShuttingDown(addr)) {
                  it.remove();
                  left_mbrs.add(addr);
                }
              }
            }

            tmp_mbrs.remove(suspected_mbrs);
            tmp_mbrs.remove(left_mbrs);
            tmp_mbrs.add(added_mbrs);
            mbrs=tmp_mbrs.getMembers();

            // putting the coordinator at the front caused problems with dlock
            // because the coordinator might think it is an elder, but the old
            // elder would not release its role, causing a deadlock.  See #47562
//            if (mbrs.contains(local_addr) && !mbrs.get(0).equals(local_addr)) {
//              mbrs.remove(local_addr);
//              mbrs.add(0, this.local_addr);
//            }
            
            v=new View(local_addr, vid, mbrs, suspected_mbrs);

            // Update membership (see DESIGN for explanation):
            tmp_members.set(mbrs);

            // Update joining list (see DESIGN for explanation)
            if(added_mbrs != null) {
                for(int i=0; i < added_mbrs.size(); i++) {
                    tmp_mbr=(Address)added_mbrs.elementAt(i);
                    if(!joining.contains(tmp_mbr))
                        joining.addElement(tmp_mbr);
                }
            }

            // Update leaving list (see DESIGN for explanations)
            if(left_mbrs != null) {
                for(Iterator it=left_mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    if(!this.leaving.contains(addr))
                        this.leaving.add(addr);
                }
            }
            if(suspected_mbrs != null) {
                for(Iterator it=suspected_mbrs.iterator(); it.hasNext();) {
                    Address addr=(Address)it.next();
                    if(!this.leaving.contains(addr))
                        this.leaving.add(addr);
                }
            }

            if(log.isDebugEnabled()) log.debug("new view is " + v);
            return v;
        }
    }


    /**
     Compute a new view, given the current view, the new members and the suspected/left
     members. Then simply mcast the view to all members. This is different to the VS GMS protocol,
     in which we run a FLUSH protocol which tries to achive consensus on the set of messages mcast in
     the current view before proceeding to install the next view.

     The members for the new view are computed as follows:
     <pre>
     existing          leaving        suspected          joining

     1. new_view      y                 n               n                 y
     2. tmp_view      y                 y               n                 y
     (view_dest)
     </pre>

     <ol>
     <li>
     The new view to be installed includes the existing members plus the joining ones and
     excludes the leaving and suspected members.
     <li>
     A temporary view is sent down the stack as an <em>event</em>. This allows the bottom layer
     (e.g. UDP or TCP) to determine the members to which to send a multicast message. Compared
     to the new view, leaving members are <em>included</em> since they have are waiting for a
     view in which they are not members any longer before they leave. So, if we did not set a
     temporary view, joining members would not receive the view (signalling that they have been
     joined successfully). The temporary view is essentially the current view plus the joining
     members (old members are still part of the current view).
     </ol>
     * @param mcast TODO
     */
    public void castViewChange(Vector new_mbrs, Vector left_mbrs, Vector suspected_mbrs, boolean mcast) {
        View new_view;

        // next view: current mbrs + new_mbrs - old_mbrs - suspected_mbrs
        new_view=getNextView(new_mbrs, left_mbrs, suspected_mbrs);
        castViewChange(new_view, null, true);
    }


    public void castViewChange(View new_view, Digest digest, boolean mcast) {
        //boolean mcast = stack.jgmm.getDistributionConfig().getMcastPort() > 0; // GemStoneAddition
        castViewChangeWithDest(new_view, digest, null, true);
    }
    
    // GemStoneAddition - send the view with unicast to avoid NAKACK rejection of non-member
    protected void ucastViewChange(View new_view) {
      castViewChangeWithDest(new_view, null, null, false);
    }


    /**
     * Broadcasts the new view and digest, and waits for acks from all members in the list given as argument.
     * If the list is null, we take the members who are part of new_view
     * <p>GemStoneAddition much of this method was rewritten for quorum-based
     * network partition detection
     * @param new_view
     * @param digest
     * @param newMbrs_p
     * @param mcast_p whether multicast may be used (GemStoneAddition)
     */
    public void castViewChangeWithDest(View new_view, Digest digest, java.util.List newMbrs_p, boolean mcast_p) {
        GmsHeader hdr;
        long      start, stop;
        ViewId    vid=new_view.getVid();
        int       size=-1;
        
        if (this.disconnected) {
          // GemStoneAddition: during debugging #50633 ViewHandler was found to
          // be casting a new view after the stack had been disconnected.  UNICAST
          // had already reset all of its connections so everyone ignored the
          // view, but that's not guaranteed to be the case.
          return;
        }
        
        if (TEST_HOOK_SLOW_VIEW_CASTING > 0) {
          try {
            log.getLogWriter().info(ExternalStrings.DEBUG,
                "Delaying view casting by " + TEST_HOOK_SLOW_VIEW_CASTING
                + " seconds for testing");
            Thread.sleep(TEST_HOOK_SLOW_VIEW_CASTING * 1000);
          } catch (InterruptedException e) {
            log.getLogWriter().fine("Test hook interrupted while sleeping");
            Thread.currentThread().interrupt();
          } finally {
            TEST_HOOK_SLOW_VIEW_CASTING = 0;
          }
        }

        // GemStoneAddition
        // we've already sent a tmp_view down the stack to inform comm procotols
        // about new members.  This tmp_view notification will remove old members
        // as well so that address canonicalization will not pick up old addresses
        // if a new member reuses an addr:port of an old member
        passDown(new Event(Event.TMP_VIEW, new_view));

        // do not multicast the view unless we really have a multicast port.  Otherwise
        // we are relying on weak messaging support in TP based on the last installed view
        // see bug #39429
        boolean mcast = mcast_p && stack.gfPeerFunctions.getMcastPort() > 0; // GemStoneAddition

        List newMbrs = newMbrs_p; // GemStoneAddition - don't update formal parameters
        if(newMbrs == null || newMbrs.size() == 0) {
            newMbrs=new LinkedList(new_view.getMembers());
        }
        
        Set suspects = new HashSet(new_view.getSuspectedMembers());
        for (Iterator it=suspects.iterator(); it.hasNext(); ) {
          IpAddress addr = (IpAddress)it.next();
          if (this.stack.gfPeerFunctions.isShuttingDown(addr)) { // GemStoneAddition bug #44342
            new_view.notSuspect(addr);
          }
        }

        log.getLogWriter().info(ExternalStrings.GMS_MEMBERSHIP_SENDING_NEW_VIEW_0_1_MBRS, 
            new Object[] { new_view, Integer.valueOf(new_view.size())}/*, new Exception("STACK")*/);

        start=System.currentTimeMillis();
        hdr=new GmsHeader(GmsHeader.VIEW);
        new_view.setMessageDigest(digest); // GemStoneAddition - move digest to message body
//        hdr.my_digest=digest;
        try {
          // in 7.0.1 we switch to always performing 2-phased view installation and quorum checks
          if (!prepareView(new_view, mcast, newMbrs)) {
            return;
          }

        synchronized (ack_collector) { // GemStoneAddition bug34505
          ack_collector.reset(vid, newMbrs);
          size=ack_collector.size();
        }

        // GemStoneAddition - when PWNing an existing view, we have to unicast
        // because NAKACK will reject a mcast message from a non-member
        if (mcast) {
          Message view_change_msg=new Message(); // bcast to all members
          view_change_msg.isHighPriority = true;
          view_change_msg.putHeader(name, hdr);
          view_change_msg.setObject(new_view);
          passDown(new Event(Event.MSG, view_change_msg));
        }
        else {
          for (int i=0; i<newMbrs.size(); i++) {
            Message msg = new Message();
            msg.isHighPriority = true;
            msg.putHeader(name, hdr);
            msg.setObject(new_view);
            msg.setDest((Address)newMbrs.get(i));
            passDown(new Event(Event.MSG, msg));
          }
        }
        // also send the view to any suspect members to give them a
        // chance to exit.  bypass use of UNICAST since it will refuse
        // to send a message to a non-member
        if (new_view.getSuspectedMembers() != null) {
          for (Iterator it=new_view.getSuspectedMembers().iterator(); it.hasNext(); ) {
            Message msg = new Message();
            msg.isHighPriority = true;
            msg.putHeader(name, hdr);
            msg.putHeader(UNICAST.BYPASS_UNICAST, hdr);
            msg.setDest((Address)it.next());
            passDown(new Event(Event.MSG, msg));
          }
        }
//        if (new_view.getSuspectedMembers().size() > 0 && log.getLogWriterI18n().fineEnabled()) {
//          log.getLogWriterI18n().warning(
//              JGroupsStrings.DEBUG, "bogus warning to test failure mode"
//          );
//        }
        try {
          ack_collector.waitForAllAcks(view_ack_collection_timeout);
          if(trace) {
              stop=System.currentTimeMillis();
              log.trace("received all ACKs (" + size + ") for " + vid + " in " + (stop-start) + "ms");
          }
        }
        catch(TimeoutException e) {
          // GemStoneAddition - bug35218
          String missingStr;
          String receivedStr;
          synchronized (ack_collector) {
            missingStr = ack_collector.getMissingAcks().toString();
            receivedStr = ack_collector.getReceived().toString();
          }
          // In 8.0 this is changed to not do suspect processing if the coordinator is shutting down
          if (!stack.getChannel().closing()) {
            log.getLogWriter().warning(
              ExternalStrings.GMS_FAILED_TO_COLLECT_ALL_ACKS_0_FOR_VIEW_1_AFTER_2_MS_MISSING_ACKS_FROM_3_RECEIVED_4_LOCAL_ADDR_5,
              new Object[] {Integer.valueOf(size), vid, Long.valueOf(view_ack_collection_timeout), missingStr/*, receivedStr, local_addr*/});
            /*
             * GemStoneAddition
             * suspect members that did not respond.  This must be done in
             * a different thread to avoid lockups in FD_SOCK
             */
            /*new Thread() {
              @Override // GemStoneAddition  
              public void run() {
                // bug #49448 fixed here by not synchronizing on ack-collector
                // while suspect verification processing is going on
                List suspects = ack_collector.getMissingAcks();
                suspect(suspects);
              }}.start();*/
            checkFailedToAckMembers(new_view, ack_collector.getMissingAcks());
          }
        }
      }
      finally {
        // GemStoneAddition - now we can fully reset the collector so that
        // it is ready to receive view-acks from join responses subsequent
        // to this view.  Prior to this change, these acks were lost when
        // the view was reset in this method
        synchronized(ack_collector) {
          ack_collector.fullyReset();
        }
        synchronized(prepare_collector) {
          prepare_collector.fullyReset(); // GemStoneAddition - fix for #43886
        }
        // GemStoneAddition - we don't need this anymore
        this.previousPreparedView = null;
      }
    }
    

    
    /**
     * GemStoneAddition - send a prepare to all members and require a response
     * 
     * @param new_view
     * @param mcast
     * @param newMbrs
     * @return true if the view can be transmitted
     */
    boolean prepareView(View new_view, boolean mcast, List newMbrs) {

      // compute the failed weight and form a set of non-admin failures
      Set<IpAddress> failures = new HashSet(new_view.getSuspectedMembers());
      int failedWeight = processFailuresAndGetWeight(this.view, this.leader, failures);

//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "[partition detection] these cache processes failed: " + failures);
      
      // this.preparedView may have been set before this vm decided to send out
      // a view.  If it was, we use it to modify the current view and try again
      if (!processPreparedView(this.preparedView, new_view, newMbrs, false)) {
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("processPreparedView failed; aborting view " + new_view.getVid());
        }
        return false;
      }
      
      
      List<IpAddress> failedToAck = sendPrepareForViewChange(new_view, newMbrs, false);

      // this.preparedView may have been set by a PREPARE_VIEW_ACK response
      // if another member received a PREPARE_VIEW with a different view than
      // we just tried to prepare
      if (!processPreparedView(this.preparedView, new_view, newMbrs, false)) {
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("processPreparedView failed; aborting view " + new_view.getVid());
        }
        return false;
      }
      
      // [bruce] prior to 7.0.1 we did not perform suspect processing on members that fail to
      // respond to a new view.  In 7.0.1 we require timely responses to view changes
      // in order to detect loss of quorum.
      // In 8.0 this is changed to not do suspect processing if the coordinator is shutting down
      if (failedToAck.size() > 0 && !stack.getChannel().closing()) {
        if ( !checkFailedToAckMembers(new_view, failedToAck) ) {
          return false;
        }
      }
      if (this.joined) { // bug #44491 - don't declare a network partition if we're still starting up 
        // okay, others have acked that they're ready for the view, so we know
        // at this point who was around after loss of the members in the failures set
        int oldWeight = 0;
        //      Map<IpAddress, Integer> oldWeights = new HashMap<IpAddress, Integer>();
        boolean leadProcessed = false;
        boolean displayWeights = (failedWeight > 0) && !Boolean.getBoolean("gemfire.hide-member-weights");
        StringBuffer sb = displayWeights? new StringBuffer(1000) : null;
        for (Iterator it = this.view.getMembers().iterator(); it.hasNext(); ) {
          IpAddress a = (IpAddress)it.next();
          int thisWeight = a.getMemberWeight();
          if (a.getVmKind() == 10 /* NORMAL_DM_KIND */) {
            thisWeight += 10;
            if (!leadProcessed) {
              thisWeight += 5;
              leadProcessed = true;
            }
          } else if (a.preferredForCoordinator()) {
            thisWeight += 3;
          }
          oldWeight += thisWeight;
          if (displayWeights) {
            sb.append("\n")
            .append(ExternalStrings.GMS_MEMBER_0_HAS_WEIGHT_1
                .toLocalizedString(a, Integer.valueOf(thisWeight)));
          }
        }
        if (sb != null) {
          String str = sb.toString();
          if (str.length() > 0) {
            log.getLogWriter().info(GFStringIdImpl.LITERAL, str);
          }
        }
        int lossThreshold = (int)Math.round((oldWeight * this.partitionThreshold) / 100.0); 

        if (failedWeight > 0) {
          log.getLogWriter().info(ExternalStrings.NetworkPartitionDetectionWeightCalculation,
              new Object[] {Integer.valueOf(oldWeight), Integer.valueOf(failedWeight),
              Integer.valueOf(this.partitionThreshold),
              lossThreshold});

          if (failedWeight >= lossThreshold) {
            if (this.splitBrainDetectionEnabled) {
              this.networkPartitionDetected = true;
              sendNetworkPartitionWarning(new_view.getMembers());
              quorumLost(failures, this.view);
              forceDisconnect(new Event(Event.EXIT, stack.gfBasicFunctions.getForcedDisconnectException(
                  ExternalStrings.GMS_EXITING_DUE_TO_POSSIBLE_NETWORK_PARTITION_EVENT_DUE_TO_LOSS_OF_MEMBER_0
                    .toLocalizedString(failures.size(), failures))));
              return false;
            } // else quorumLost will be invoked when the view is installed
          }
        }
      }
      
      if (log.getLogWriter().fineEnabled()) {
        log.getLogWriter().fine("done successfully preparing view " + new_view.getVid());
      }
      return true;
    }
    
    private boolean checkFailedToAckMembers(final View new_view, final List<IpAddress> failedToAck) {
      
      if (failedToAck.size() == 0) {
        return true;
      }

      if (log.getLogWriter().infoEnabled()) {
        log.getLogWriter().info(ExternalStrings.CHECKING_UNRESPONSIVE_MEMBERS,
            new Object[]{failedToAck});
      }
      final FD_SOCK fdSock = (FD_SOCK)this.stack.findProtocol("FD_SOCK");
      if (this.stack.getChannel().closing() && fdSock == null) {
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("FD_SOCK not found for liveness checks - aborting view " + new_view.getVid());
        }
        return false;  // bug #44786: cannot prepare a view if the stack has been dismantled
      }
      assert fdSock != null;
      ExecutorService es = Executors.newFixedThreadPool(failedToAck.size(), new ThreadFactory() {
        private final AtomicInteger threadCount = new AtomicInteger(1);
        @Override
        public Thread newThread(Runnable r) {
          Thread th = new Thread(GemFireTracer.GROUP,
               r,
               FAILED_TO_ACK_MEMBER_VERIFY_THREAD +  "-" + threadCount.getAndIncrement()); 
          return th;
        }
      });
      ArrayList<Callable<IpAddress>> al = new ArrayList<Callable<IpAddress>>();
      for (final Iterator<IpAddress> it=failedToAck.iterator(); it.hasNext(); ) {
        final IpAddress failedAddress = it.next();
        al.add(new Callable<IpAddress>() {
          IpAddress sockAddress = fdSock.fetchPingAddress(failedAddress, 0);
          public IpAddress call() throws Exception {
            log.getLogWriter().info(ExternalStrings.SUSPECTING_MEMBER_WHICH_DIDNT_ACK, new Object[]{failedAddress.toString()});
            if (sockAddress == null) {
              if (log.getLogWriter().fineEnabled()) {
                log.getLogWriter().fine("unable to find ping address for " + failedAddress
                    + " - using direct port to verify if it's there");
              }
              // fdSock can't perform the verification because it has no FD_SOCK
              // address for the member
              // If we can connect to the member's cache socket, then we know its machine is
              // up.  It may be dead and its socket port reused, but we know there isn't a
              // network partition going on.
              sockAddress = new IpAddress(failedAddress.getIpAddress(), failedAddress.getDirectPort());
              if (sockAddress.getPort() != 0 &&
                  fdSock.checkSuspect(failedAddress, sockAddress, ExternalStrings.MEMBER_DID_NOT_ACKNOWLEDGE_VIEW.toLocalizedString(), false, false)) {
                if (log.getLogWriter().infoEnabled()) {
                  log.getLogWriter().info(ExternalStrings.ABLE_TO_CONNECT_TO_DC_PORT, new Object[]{failedAddress, sockAddress.getPort()});
                }
                return failedAddress;//now we remove below using feature
              }
            } else if (fdSock.checkSuspect(failedAddress, sockAddress, ExternalStrings.MEMBER_DID_NOT_ACKNOWLEDGE_VIEW.toLocalizedString(), true, false)) {
              if (log.getLogWriter().infoEnabled()) {
                log.getLogWriter().info(ExternalStrings.ABLE_TO_CONNECT_TO_FD_PORT, new Object[]{failedAddress, sockAddress.getPort()});
              }
              return failedAddress;//now we remove below using feature
            }
              return null;
          }
         }
        );
      }
      try {
        if (log.getLogWriter().infoEnabled()) {
          log.getLogWriter().info(ExternalStrings.CHECKING_FAILED_TO_ACK_MEMBERS, new Object[]{failedToAck.toString()});
        }
         List<java.util.concurrent.Future<IpAddress>> futures =  es.invokeAll(al);
         for(java.util.concurrent.Future<IpAddress> future : futures){
           try {
             IpAddress ipAddr = future.get(view_ack_collection_timeout  + 100, TimeUnit.MILLISECONDS);
             if(ipAddr != null) {
               failedToAck.remove(ipAddr);
             }
          } catch (ExecutionException e) {
          } catch (java.util.concurrent.TimeoutException e) {
          }
         }
        } catch (InterruptedException e) {
        }
        
        try{      
          es.shutdown();
          es.awaitTermination(view_ack_collection_timeout , TimeUnit.MILLISECONDS);
        }catch(Exception ex) {
          //ignore
        }
      // we could, at this point, also see if there are any ip addresses in the
      // set that ack'd the message that are also in the failed set.  That would
      // tell us if the route to that NIC is still viable and we could remove the
      // entry from the failedToAck set [bruce 2011]
      if (!failedToAck.isEmpty()) {
        //                  log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "these cache processes failed to ack a view preparation message: " + failedToAck);
        // abandon this view and cast a new view with the failed ackers added to the suspects list
        failedToAck.addAll(new_view.getSuspectedMembers());
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("invoking handleLeave with " + failedToAck + ".  My membership is " + this.members.getMembers());
        }
        this.impl.handleLeave(failedToAck, true,
            Collections.singletonList(ExternalStrings.MEMBER_DID_NOT_ACKNOWLEDGE_VIEW.toLocalizedString()),
            true);
        if (log.getLogWriter().fineEnabled()) {
          log.getLogWriter().fine("done casting view " + new_view.getVid());
        }
        return false;
      }
      return true;
    }
    
    /**
     * process the given set of failures against the given last known view and report the
     * amount of lost weight.  This also removes any admin members from the collection of failures.
     * 
     * @param lastView the last View that was completely installed
     * @param leader the leader process in lastView
     * @param failures the set of failures in the view being created
     * @return the amount of weight lost in the failures collection
     */
    public static int processFailuresAndGetWeight(View lastView, Address leader, Collection failures) {
      int failedWeight = 0;
      for (Iterator<IpAddress> it=failures.iterator(); it.hasNext(); ) {
        IpAddress addr = it.next();
        if (!lastView.getMembers().contains(addr)) { // bug #37342: added and removed since the last installed view
          continue;
        }
        failedWeight += addr.getMemberWeight();
        if (addr.getVmKind() != 10 /* NORMAL_DM_KIND */) {
          if (addr.preferredForCoordinator()) {
            failedWeight += 3;
          }
          it.remove();
        } else {
          failedWeight += 10;
        }
      }
      if (leader != null && failures.contains(leader)) {
        failedWeight += 5;
      }
      return failedWeight;
    }
    
    // GemStoneAddition - notify of loss of quorum
    private void quorumLost(final Set failures, final View currentView) {
      Thread notificationThread = new Thread(GemFireTracer.GROUP, "Quorum Lost Notification") {
        public void run() {
          List remaining = new ArrayList(currentView.getMembers().size());
          remaining.addAll(currentView.getMembers());
          remaining.removeAll(failures);
          try {
            stack.gfPeerFunctions.quorumLost(failures, remaining);
          } catch (RuntimeException e) {
            if (e.getClass().getSimpleName().equals("CancelException")) {
              // bug #47403: ignore this exception - cache closed before notification went through
            } else {
              throw e;
            }
          }
        }
      };
      notificationThread.setDaemon(true);
      notificationThread.start();
    }
    
    /**
     * GemStoneAddition - process a view received in a PREPARE_VIEW message from another
     * coordinator, or from a PREPARE_VIEW_ACK response to a message
     * that this node transmitted to another member
     * @param pView the view being prepared
     * @param new_view the view I am trying to cast
     * @param newMbrs the members list in new_view
     * @param mcast whether the view can be multicast
     * @return true if new_view can be installed, false if a different view was installed
     */
    boolean processPreparedView(View pView, View new_view, List newMbrs, boolean mcast) {
      View prevView = this.previousPreparedView;
      if (pView != null // different view has been prepared
          && !pView.getCreator().equals(this.local_addr) // I did not create it
          && pView.getVid().compare(this.view.getVid()) > 0 // It's newer than my current view
          && (prevView == null || pView.getVid().compare(prevView.getVid()) > 0) ) { // I haven't processed a newer prepared view
        
        this.previousPreparedView = pView;

        Vector newMembersFromPView = new Vector();
        Vector newFailuresFromPView = new Vector();
        Vector newLeavingFromPView = new Vector();
        synchronized(members) {
          // someone else has already prepared a new view
          if (log.getLogWriter().infoEnabled()) {
            log.getLogWriter().info(ExternalStrings.RECEIVED_PREVIOUSLY_PREPARED_VIEW, pView);
          }
          Set<Address> allFailures = new HashSet(this.leaving);
          for (Iterator it=pView.getMembers().iterator(); it.hasNext(); ) {
            Address a = (Address)it.next();
            if (!allFailures.contains(a)) {
              // in the other view and not in my failure list - new member
              newMembersFromPView.add(a);
            }
          }
          for (Iterator it=pView.getSuspectedMembers().iterator(); it.hasNext(); ) {
            Address a = (Address)it.next();
            if (!allFailures.contains(a)) {
              // failed in other view but not in mine
              newFailuresFromPView.add(a);
            }
          }
          for (Iterator it=newMbrs.iterator(); it.hasNext(); ) {
            Address a = (Address)it.next();
            if (!pView.containsMember(a)) {
              newLeavingFromPView.add(a);
            }
          }
        }
        if (!newMembersFromPView.isEmpty()
            || !newFailuresFromPView.isEmpty() 
            || !newLeavingFromPView.isEmpty()) {
          // we need the full list of failures but the others are just increments on
          // the leaving/joining collections in tmp_mbrs
          newFailuresFromPView.addAll(new_view.getSuspectedMembers());
          synchronized(prepare_collector) {
            prepare_collector.fullyReset(); // GemStoneAddition - fix for #43886
          }
          castViewChange(newMembersFromPView, newLeavingFromPView, newFailuresFromPView, mcast);
          return false;
        }
      }
      return true;
    }
    
    /**
     * GemStoneAddition - send prepare-for-view-change message and wait for
     * responses.  Return a list of addresses that didn't respond
     */
    List<IpAddress> sendPrepareForViewChange(View newView, Collection<IpAddress> mbrs, boolean mcast) {
      int size=-1;
      ViewId vid = newView.getVid();

      synchronized (prepare_collector) { // GemStoneAddition bug34505
        prepare_collector.reset(vid, mbrs);
        size=prepare_collector.size();
      }
      boolean timedout=false;
      GmsHeader hdr=new GmsHeader(GmsHeader.PREPARE_FOR_VIEW);
      // GemStoneAddition - when PWNing an existing view, we have to unicast
      // because NAKACK will reject a mcast message from a non-member
      if (mcast) {
        Message view_change_msg=new Message(); // bcast to all members
        view_change_msg.setObject(newView);
        view_change_msg.isHighPriority = true;
        view_change_msg.putHeader(name, hdr);
        passDown(new Event(Event.MSG, view_change_msg));
      }
      else {
        for (IpAddress dest: mbrs) {
          if (dest.equals(this.local_addr)) {
            synchronized(prepare_collector) {
              prepare_collector.ack(dest, null);
            }
          } else {
            Message msg = new Message();
            msg.isHighPriority = true;
            msg.putHeader(name, hdr);
            msg.setObject(newView);
            msg.setDest(dest);
//            if (trace)
//              log.trace("sending PREPARE_FOR_VIEW to " + dest);
            passDown(new Event(Event.MSG, msg));
          }
        }
      }
      try {
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "DEBUG: before waiting for prepare-acks, collector state is " + prepare_collector);
        prepare_collector.waitForAllAcks(view_ack_collection_timeout);
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "DEBUG: after waiting for prepare-acks, collector state is " + prepare_collector);
      }
      catch(TimeoutException e) {
        // timeout processing is handled below, along with members declared dead during view preparation
        // see bug #47295
        timedout=true;
      }
      //log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "received acks from " + prepare_collector.getReceived());
      List missing;
      String missingStr;
      String receivedStr = null;
      boolean logMissing = false;
      synchronized (prepare_collector) {
        missing = prepare_collector.getMissingAcks();
        missing.addAll(prepare_collector.getSuspectedMembers());
        missing.removeAll(newView.getSuspectedMembers()); // don't reprocess members that are already kicked out
        missing.remove(this.local_addr); // don't suspect myself - I might be shutting down
        // bug #44342 - if the member has sent a shutdown message then it's okay
        for (Iterator<IpAddress> it=missing.iterator(); it.hasNext(); ) {
          IpAddress addr = it.next();
          if (!newView.containsMember(addr) || this.stack.gfPeerFunctions.isShuttingDown(addr)) {
            it.remove();
          }
        }
        logMissing = missing.size() > 0;
        if (logMissing) {
          // note who has acked while under synchronization
          receivedStr = prepare_collector.getReceived().toString();
        }
      }
      if (logMissing  && !this.stack.getChannel().closing()) {
        missingStr = missing.toString();
        log.getLogWriter().warning(
          ExternalStrings.GMS_FAILED_TO_COLLECT_ALL_ACKS_0_FOR_VIEW_PREPARATION_1_AFTER_2_MS_MISSING_ACKS_FROM_3_RECEIVED_4_LOCAL_ADDR_5,
          new Object[] {Integer.valueOf(size), newView, Long.valueOf(view_ack_collection_timeout), missingStr, receivedStr, local_addr}/*, new Exception("stack trace")*/);
      }
      return missing; 
    }
    

    void sendNetworkPartitionWarning(Collection<IpAddress> mbrs) {
      GmsHeader hdr = new GmsHeader(GmsHeader.NETWORK_PARTITION_DETECTED);
      for (IpAddress dest: mbrs) {
        Message msg = new Message();
        msg.isHighPriority = true;
        msg.putHeader(name, hdr);
        msg.setDest(dest);
        passDown(new Event(Event.MSG, msg));
      }
      try {
        Thread.sleep(this.leave_timeout);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // pass the problem to other code
      }
    }
    
    /**
     * GemStoneAddition - suspect members that fail to return view acks
     */
    protected void suspect(List suspects) {
      passDown(new Event(Event.GMS_SUSPECT, suspects));
    }


    /**
     * GemStoneAddition - tell the ack collectors to stop waiting for the
     * given address
     */
    public void addSuspectToCollectors(Address addr) {
      synchronized (ack_collector) {
        ack_collector.suspect(addr);
      }
      synchronized(prepare_collector) {
        prepare_collector.suspect(addr);
      }
    }
    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack. If the view is a MergeView (subclass
     * of View), then digest will be non-null and has to be set before installing the view.
     */
    public void installView(View new_view, Digest digest) {
        if(digest != null)
            mergeDigest(digest);
        installView(new_view);
    }

    
    /** GemStoneAddition - return the weight of the given IDs */
    public static int getWeight(Collection ids, Address leader) {
      int weight = 0;
      for (Iterator<IpAddress> it=ids.iterator(); it.hasNext(); ) {
        IpAddress addr = it.next();
        int thisWeight = addr.getMemberWeight();
        if (addr.getVmKind() == 10 /* NORMAL_DM_KIND */) {
          thisWeight += 10;
          if (leader != null && addr.equals(leader)) {
            thisWeight += 5;
          }
        } else if (addr.preferredForCoordinator()) {
          thisWeight += 3;
        }
        weight += thisWeight;
      }
      return weight;
    }

    /**
     * Sets the new view and sends a VIEW_CHANGE event up and down the stack.
     */
    public void installView(View new_view) {
        Address coord;
        int rc;
        ViewId vid=new_view.getVid();
        Vector mbrs=new_view.getMembers();

        if (networkPartitionDetected) {
          return; // GemStoneAddition - do not install the view if we've decided to exit
        }
        this.preparedView = null;

        if(log.isDebugEnabled()) log.debug("[local_addr=" + local_addr + "] view is " + new_view);
//log.getLogWriter().info("installing view " + new_view); // debugging
        if(stats) {
            num_views++;
            prev_views.add(new_view);
        }

        // Discards view with id lower than our own. Will be installed without check if first view
        if(view_id != null) {
            rc=vid.compareTo(view_id);
            if(rc <= 0) {
                if(log.isTraceEnabled() && rc < 0) // only scream if view is smaller, silently discard same views
                    log.trace("[" + local_addr + "] received view < current view;" +
                            " discarding it (current vid: " + view_id + ", new vid: " + vid + ')');
                return;
            }
        }

        ltime=Math.max(vid.getId(), ltime);  // compute Lamport logical time

        /* Check for self-inclusion: if I'm not part of the new membership, I just discard it.
        This ensures that messages sent in view V1 are only received by members of V1 */
        if(checkSelfInclusion(mbrs) == false) {
            // only shun if this member was previously part of the group. avoids problem where multiple
            // members (e.g. X,Y,Z) join {A,B} concurrently, X is joined first, and Y and Z get view
            // {A,B,X}, which would cause Y and Z to be shunned as they are not part of the membership
            // bela Nov 20 2003
            if(shun && local_addr != null && prev_members.contains(local_addr)) {
                if(warn)
                    log.warn("I (" + local_addr + ") am not a member of view " + new_view +
                            ", shunning myself and leaving the group (prev_members are " + prev_members +
                            ", current view is " + view + ")");
                if(impl != null)
                    impl.handleExit();
                networkPartitionDetected = true;
                passUp(new Event(Event.EXIT,
                    stack.gfBasicFunctions.getForcedDisconnectException(
                        "This member has been forced out of the distributed system by " + new_view.getCreator()
                        + ".  Please consult GemFire logs to find the reason. (GMS shun)")));
            }
            else {
                if(warn) log.warn("I (" + local_addr + ") am not a member of view " + new_view + "; discarding view");
            }
            return;
        }
        synchronized(installViewLock) {
          synchronized(members) {   // serialize access to views
              
              if (this.view != null) {
                int oldWeight = getWeight(this.view.getMembers(), this.leader);
                int failedWeight = getWeight(new_view.getSuspectedMembers(), this.leader);
                int lossThreshold = (int)Math.round((oldWeight * this.partitionThreshold) / 100.0); 
                if (failedWeight >= lossThreshold) {
                  log.getLogWriter().info(ExternalStrings.DEBUG, "old membership weight=" + oldWeight + ", loss threshold=" + lossThreshold+" and failed weight=" + failedWeight);
//                  log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "current view="+this.view+"; suspects="+new_view.getSuspectedMembers());
                  quorumLost(new_view.getSuspectedMembers(), this.view);
                }
              }
  
              // assign new_view to view_id
              if(new_view instanceof MergeView)
                  view=new View(new_view.getVid(), new_view.getMembers());
              else
                  view=new_view;
              view_id=vid.copy();

              // GemStoneAddition - tracking of leader
              Address oldLead = this.leader;
              this.leader = view.getLeadMember();
              if (this.leader != null) {
                // log acquisition of new leader
                if (oldLead == null || !oldLead.equals(this.leader)) {
                  log.getLogWriter().info(ExternalStrings.GMS_MEMBERSHIP_LEADER_MEMBER_IS_NOW_0, this.leader);
                }
              }
  
  
              // Set the membership. Take into account joining members
              if(mbrs != null && mbrs.size() > 0) {
                  members.set(mbrs);
                  this.coordinator = members.getCoordinator(); // GemStoneAddition bug #44967
                  tmp_members.set(members);
                  joining.removeAll(mbrs);  // remove all members in mbrs from joining
                  // remove all elements from 'leaving' that are not in 'mbrs'
                  leaving.retainAll(mbrs);
  
                  tmp_members.add(joining);    // add members that haven't yet shown up in the membership
                  tmp_members.remove(leaving); // remove members that haven't yet been removed from the membership
  
                  // add to prev_members
                  for(Iterator it=mbrs.iterator(); it.hasNext();) {
                      Address addr=(Address)it.next();
                      if(!prev_members.contains(addr))
                          prev_members.add(addr);
                  }
              }
  
              // Send VIEW_CHANGE event up and down the stack:
              Event view_event=new Event(Event.VIEW_CHANGE, new_view.clone());
              passDown(view_event); // needed e.g. by failure detector or UDP
              passUp(view_event);
          }
            coord=determineCoordinator();
            // if(coord != null && coord.equals(local_addr) && !(coord.equals(vid.getCoordAddress()))) {
            // changed on suggestion by yaronr and Nicolas Piedeloupe
            if(coord != null && coord.equals(local_addr) && !haveCoordinatorRole()) {
                // GemSToneAddition - pass suspects to coordinator impl
                Vector suspects;
                if (haveParticipantRole()) {
                  suspects = ((ParticipantGmsImpl)impl).getSuspects();
                } else {
                  suspects = new Vector();
                }
                becomeCoordinator(suspects);
                coord = local_addr; // GemStoneAddition
            }
            else {
                if(haveCoordinatorRole() && !local_addr.equals(coord))
                    becomeParticipant();
            }
            // GemStoneAddition - notify concerned parties of the current coordinator
            if (coord != null) {
              notifyOfCoordinator(coord);
            }
        }
    }
    
    
    void forceDisconnect(final Event exitEvent) {
      this.networkPartitionDetected = true;
      Thread tilt = new Thread(Thread.currentThread().getThreadGroup(),
          "GMS Network Partition Event") {
        @Override // GemStoneAddition  
        public void run() {
          passDown(exitEvent);
          passUp(exitEvent);
        }
      };
      tilt.start();
    }

    /**
     * GemStoneAddition - rewritten for localizing view coordinator in locator
     * processes, and made public
     * @return the acting coordinator
     */
    public Address determineCoordinator() {
//            return members != null && members.size() > 0? (Address)members.elementAt(0) : null;
        return this.coordinator;
    }
    
    /**
     * GemStoneAddition - return the lead member for partition detection algorithms
     */
    public Address getLeadMember() {
      return this.leader;
    }
    
    /**
     * GemStoneAddition - retreive the partitionThreshold for quorum calculations
     */
    public int getPartitionThreshold() {
      return this.partitionThreshold;
    }
    
    /**
     * GemStoneAddition - retrieve the latest view
     */
    public View getLastView() {
      return this.view;
    }

    /** Checks whether the potential_new_coord would be the new coordinator (2nd in line) */
    protected boolean wouldBeNewCoordinator(Address potential_new_coord) {
//        Address new_coord;

        if(potential_new_coord == null) return false;

        synchronized(members) {
            if(members.size() < 2) return false;
//            new_coord=(Address)members.elementAt(1);  // member at 2nd place
//            return new_coord != null && new_coord.equals(potential_new_coord);
            return members.wouldBeNewCoordinator(potential_new_coord);
        }
    }


    /** Returns true if local_addr is member of mbrs, else false */
    protected boolean checkSelfInclusion(Vector mbrs) {
        Object mbr;
        if(mbrs == null)
            return false;
        for(int i=0; i < mbrs.size(); i++) {
            mbr=mbrs.elementAt(i);
            if(mbr != null && local_addr.equals(mbr))
                return true;
        }
        return false;
    }


    // GemStoneAddition - this method is not used by jgroups
//    public View makeView(Vector mbrs) {
//        Address coord=null;
//        long id=0;
//
//        if(view_id != null) {
//            coord=view_id.getCoordAddress();
//            id=view_id.getId();
//        }
//        return new View(coord, id, mbrs);
//    }


    // GemStoneAddition - this method is not used by jgroups
//    public View makeView(Vector mbrs, ViewId vid) {
//        Address coord=null;
//        long id=0;
//
//        if(vid != null) {
//            coord=vid.getCoordAddress();
//            id=vid.getId();
//        }
//        return new View(coord, id, mbrs);
//    }


    /** Send down a SET_DIGEST event */
    public void setDigest(Digest d) {
        passDown(new Event(Event.SET_DIGEST, d));
    }


    /** Send down a MERGE_DIGEST event */
    public void mergeDigest(Digest d) {
        passDown(new Event(Event.MERGE_DIGEST, d));
    }


    /** Sends down a GET_DIGEST event and waits for the GET_DIGEST_OK response, or
     timeout, whichever occurs first */
    public Digest getDigest() {
        Digest ret=null;

        synchronized(digest_mutex) {
            digest_promise.reset();
            passDown(Event.GET_DIGEST_EVT);
            try {
                ret=(Digest)digest_promise.getResultWithTimeout(digest_timeout);
            }
            catch(TimeoutException e) {
                if(log.isErrorEnabled()) log.error(ExternalStrings.GMS_DIGEST_COULD_NOT_BE_FETCHED_FROM_BELOW);
            }
            return ret;
        }
    }


    @Override // GemStoneAddition  
    public void up(Event evt) {
        Object obj;
        Message msg;
        GmsHeader hdr;
        MergeData merge_data;

        switch(evt.getType()) {

            case Event.MSG:
                msg=(Message)evt.getArg();
                obj=msg.getHeader(name);
                if(obj == null || !(obj instanceof GmsHeader))
                    break;
                hdr=(GmsHeader)msg.removeHeader(name);
                switch(hdr.type) {
                    case GmsHeader.JOIN_REQ:
                      if (this.haveCoordinatorRole()) {
                        // GemStoneAddition - partial fix for bugs #41722 and #42009
                        IpAddress iaddr = (IpAddress)hdr.mbr;
                        if (iaddr.getBirthViewId() >= 0 && this.stack.gfPeerFunctions.isShunnedMemberNoSync(iaddr)) {
                          log.getLogWriter().info(
                            ExternalStrings. COORDGMSIMPL_REJECTING_0_DUE_TO_REUSED_IDENTITY, hdr.mbr);
                          ((CoordGmsImpl)getImpl()).sendJoinResponse(new JoinRsp(JoinRsp.SHUNNED_ADDRESS), hdr.mbr, false);
                          return;
                        }
                        if (members.contains(hdr.mbr)) {
                          IpAddress hmbr = (IpAddress)hdr.mbr;
                          for (Iterator it=members.getMembers().iterator(); it.hasNext(); ) {
                            IpAddress addr = (IpAddress)it.next();
                            if (addr.equals(hdr.mbr)) {
                              if (addr.getUniqueID() != hmbr.getUniqueID()) {
                                log.getLogWriter().info(
                                    ExternalStrings. COORDGMSIMPL_REJECTING_0_DUE_TO_REUSED_IDENTITY, hdr.mbr);
                                  ((CoordGmsImpl)getImpl()).sendJoinResponse(new JoinRsp(JoinRsp.SHUNNED_ADDRESS), hdr.mbr, false);
                                  return;
                              } else {
                                break;
                              }
                            }
                          }
                          getImpl().handleAlreadyJoined(hdr.mbr);
                          return;
                        }
                      }
                      // GemStoneAddition - bug #50510, 50742
                      if (hdr.mbr.getVersionOrdinal() > 0
                          && hdr.mbr.getVersionOrdinal() < JGroupsVersion.CURRENT_ORDINAL) {
                        log.getLogWriter().warning(
                            ExternalStrings.COORD_REJECTING_OLD_MEMBER_BECAUSE_UPGRADE_HAS_BEGUN,
                            new Object[]{hdr.mbr});
                        ((CoordGmsImpl)getImpl()).sendJoinResponse(
                            new JoinRsp(ExternalStrings.COORD_REJECTING_OLD_MEMBER_BECAUSE_UPGRADE_HAS_BEGUN
                                .toLocalizedString(hdr.mbr)), hdr.mbr, false);
                        return;
                      }
                      view_handler.add(new Request(Request.JOIN, hdr.mbr, false, null));
                      break;
                    case GmsHeader.JOIN_RSP:
//        log.getLogWriterI18n().info(JGroupsStrings.DEBUG, "join response message received: " + hdr.join_rsp);
                        impl.handleJoinResponse((JoinRsp)msg.getObject());
                        break;
                    case GmsHeader.LEAVE_REQ:
                        if(log.isDebugEnabled())
                            log.debug("received LEAVE_REQ for " + hdr.mbr + " from " + msg.getSrc());
                        if(hdr.mbr == null) {
                            if(log.isErrorEnabled()) log.error(ExternalStrings.GMS_LEAVE_REQS_MBR_FIELD_IS_NULL);
                            return;
                        }
                        synchronized(ack_collector) {
                          ack_collector.ack(hdr.mbr, null); // GemStoneAddition - don't wait for acks from this member
                        }
                        synchronized(prepare_collector) {
                          prepare_collector.ack(hdr.mbr, null); // GemStoneAddition - don't wait for acks from this member (bug #44342)
                        }
                        view_handler.add(new Request(Request.LEAVE, hdr.mbr, false, null));
                        break;
                    case GmsHeader.LEAVE_RSP:
                        // GemStoneAddition
                        if (hdr.arg != null) {
                          // do some checking to prevent a message intended for another process
                          // from shutting this one down
                          if (hdr.mbr != null && !hdr.mbr.equals(this.local_addr)) {
                            break;
                          }
                        }
                        impl.handleLeaveResponse(hdr.arg);
                        break;
                    case GmsHeader.REMOVE_REQ: // GemStoneAddition - new REMOVE_REQ request for slow receivers
                      // REMOVE_REQ differs from LEAVE_REQ in that the member is suspect
                      // and will register as a failure in network partition detection
                      // algorithms [bruce]
                      if(hdr.mbr == null) {
                          if(log.isErrorEnabled()) log.error("REMOVE_REQ's mbr field is null");
                          return;
                      }
                      if (members.contains(msg.getSrc())) {
                        if (msg.getSrc().equals(hdr.mbr) && log.isDebugEnabled()) {
                          log.debug("received REMOVE_REQ for " + hdr.mbr + " from " + msg.getSrc() +
                              (hdr.arg == null? "" : " Reason=" + hdr.arg));
                        }
                        else {
                          log.getLogWriter().warning(
                              ExternalStrings.GMS_MEMBERSHIP_RECEIVED_REQUEST_TO_REMOVE_0_FROM_1_2,
                              new Object[] { hdr.mbr, msg.getSrc(), (hdr.arg == null? "" : " Reason=" + hdr.arg)});
                              
                        }
                        view_handler.add(new Request(Request.LEAVE, hdr.mbr, true, null, hdr.arg));
                      }
                      break;
                    case GmsHeader.VIEW:
                        // send VIEW_ACK to sender of view
                        Address coord=msg.getSrc();
                        Message view_ack=new Message(coord, null, null);
                        view_ack.isHighPriority = true;
                        View v = msg.getObject();
                        GmsHeader tmphdr=new GmsHeader(GmsHeader.VIEW_ACK);
                        view_ack.putHeader(name, tmphdr);
                        view_ack.setObject(v);
                        if (this.local_addr.getBirthViewId() < 0) {
                          // unicast can't handle changing view IDs very well
                          view_ack.putHeader(UNICAST.BYPASS_UNICAST, tmphdr);
                        }
                        passDown(new Event(Event.MSG, view_ack));
                        // GemStoneAddition - perform null check AFTER sending an ack
                        // so we don't stall a coordinator when join_rsp hasn't been
                        // received
                        if(v == null) {
                          if(log.isErrorEnabled()) log.error(ExternalStrings.GMS_VIEW_VIEW__NULL);
                          return;
                        }
                        
                        //                        ViewId newId = hdr.view.getVid(); GemStoneAddition
                        if (this.impl != null && !(this.impl instanceof ClientGmsImpl)
                            && v.getVid().compareTo(view_id) > 0
                            && !this.stack.getChannel().closing() /* GemStoneAddition - fix for bug #42969*/) {
                          log.getLogWriter().info(ExternalStrings.GMS_MEMBERSHIP_RECEIVED_NEW_VIEW__0, v);
                          impl.handleViewChange(v, v.getMessageDigest());
                        }
                        break;

                    case GmsHeader.VIEW_ACK:
                        Object sender=msg.getSrc();
                        if (trace) // GemStoneAddition - debug 34750
                          log.trace("Received VIEW_ACK from " + sender);
                        v = msg.getObject(); // GemStoneAddition - ack the correct view
                        ViewId vid = v==null? null : v.getVid();
                        synchronized (ack_collector) { // GemStoneAddition bug34505
                          ack_collector.ack(sender, vid);
                        }
                        return; // don't pass further up
                    
                    
                    case GmsHeader.PREPARE_FOR_VIEW: // GemStoneAddition
                      v = msg.getObject();
                      if (trace)
                        log.trace("Received PREPARE_FOR_VIEW from " + msg.getSrc()+" for view " + v);
                        {
                          GmsHeader responseHeader = new GmsHeader(GmsHeader.PREPARE_FOR_VIEW_ACK);
                          View responseView = null;
                          if (!msg.getSrc().equals(this.local_addr)) {
                            if (this.preparedView != null && !v.getCreator().equals(this.preparedView.getCreator())) {
                              // already have a prepared view - don't accept this one
                              responseView = this.preparedView;
                              this.preparedView = null;
                            } else {
                              this.preparedView = v;
                            }
                          }
                          Message m = new Message(true);
                          m.putHeader(name, responseHeader);
                          m.setDest(msg.getSrc());
                          m.setObject(responseView);
                          passDown(new Event(Event.MSG, m));
                          return;
                        }

                    case GmsHeader.PREPARE_FOR_VIEW_ACK: // GemStoneAddition - two phase views
                    {
                      sender=msg.getSrc();
                      v = (View)msg.getObject();
                      if (trace) {
                        String preparedViewString = v == null? "" : 
                          " with conflicting view " + v;
                        log.trace("Received PREPARE_FOR_VIEW_ACK from " + sender + preparedViewString);
                      }
                      vid = null;
                      if (v != null) {
                        // the sender had a conflicting view in preparation
                        this.preparedView = v;
                        vid = v.getVid();
                      }
                      synchronized (prepare_collector) {
                        prepare_collector.ack(sender, vid);
                      }
                      return; // don't pass further up
                    } 
                    case GmsHeader.NETWORK_PARTITION_DETECTED: // GemStoneAddition
                    {
                      forceDisconnect(new Event(Event.EXIT, stack.gfBasicFunctions.getForcedDisconnectException(ExternalStrings.COORDINATOR_DECLARED_NETWORK_PARTITION_EVENT.toLocalizedString(msg.getSrc()))));
                      return;
                    }
                    // GemStoneAddition - explicit view request
                    case GmsHeader.GET_VIEW:
                    {
                      View msgView = this.view;
                      if (msgView != null) {
                        Message viewRsp = new Message();
                        JoinRsp vrsp = new JoinRsp(msgView, getDigest());
                        GmsHeader ghdr = new GmsHeader(GmsHeader.GET_VIEW_RSP);
                        viewRsp.putHeader(name, ghdr);
                        viewRsp.setDest(msg.getSrc());
                        viewRsp.setObject(vrsp);
                        passDown(new Event(Event.MSG, viewRsp));
                        log.getLogWriter().info(ExternalStrings.DEBUG, "Sending membership view to " + viewRsp.getDest() + ": " + msgView);
                      }
                      break;
                    } 
                    // GemStoneAddition - response to view request
                    case GmsHeader.GET_VIEW_RSP:
                    {
                      impl.handleGetViewResponse((JoinRsp)msg.getObject());
                      break;
                    }
                    case GmsHeader.MERGE_REQ:
                        impl.handleMergeRequest(msg.getSrc(), hdr.merge_id);
                        break;

                    case GmsHeader.MERGE_RSP:
                      View theView = msg.getObject();
                        merge_data=new MergeData(msg.getSrc(), theView, theView.getMessageDigest());
                        merge_data.merge_rejected=hdr.merge_rejected;
                        impl.handleMergeResponse(merge_data, hdr.merge_id);
                        break;

                    case GmsHeader.INSTALL_MERGE_VIEW:
                      theView = msg.getObject();
                        impl.handleMergeView(new MergeData(msg.getSrc(), theView, theView.getMessageDigest()), hdr.merge_id);
                        break;

                    case GmsHeader.CANCEL_MERGE:
                        impl.handleMergeCancelled(hdr.merge_id);
                        break;

                    default:
                        if(log.isErrorEnabled()) log.error(ExternalStrings.GMS_GMSHEADER_WITH_TYPE_0__NOT_KNOWN, hdr.type);
                }
                return;  // don't pass up

            case Event.CONNECT_OK:     // sent by someone else, but WE are responsible for sending this !
            case Event.DISCONNECT_OK:  // dito (e.g. sent by TP layer). Don't send up the stack
                return;


            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                // GemStoneAddition - the address must hold the split-brain enabled flag
                if (local_addr instanceof IpAddress) {
                  ((IpAddress)local_addr).splitBrainEnabled(this.splitBrainDetectionEnabled);
                  ((IpAddress)local_addr).setMemberWeight(this.memberWeight);
                }
                notifyOfLocalAddress(local_addr);
                if(print_local_addr) {
                    System.out.println("\n-------------------------------------------------------\n" +
                                       "GMS: address is " + local_addr +
                                       "\n-------------------------------------------------------");
                }
                break;                               // pass up

            case Event.SUSPECT:
              SuspectMember sm = (SuspectMember)evt.getArg(); // GemStoneAddition
                Address suspected=sm.suspectedMember;
                if (!members.containsExt(suspected)) {
                  break; // not a member so don't process it
                }
                view_handler.add(new Request(Request.LEAVE, suspected, true, null, "did not respond to are-you-dead messages"));
                addSuspectToCollectors(suspected);
                break;                               // pass up

            case Event.FD_SOCK_MEMBER_LEFT_NORMALLY: // GemStoneAddition - pretty much the same as SUSPECT
              Address addr = (Address)evt.getArg();
              view_handler.add(new Request(Request.LEAVE, addr, false, null, "closed connection to FD_SOCK watcher"));
              addSuspectToCollectors(addr);
                break;                               // pass up

            case Event.UNSUSPECT:
                suspected = (Address)evt.getArg();
                impl.unsuspect(suspected);
                // GemStoneAddition - do not wait for view acknowledgement
                // from a member that has proved it is alive.  This ensures
                // that we don't wait inordinately long for a member that
                // might just be extremely busy.
                addSuspectToCollectors(suspected);
                return;                              // discard

            case Event.MERGE:
                view_handler.add(new Request(Request.MERGE, null, false, (Vector)evt.getArg()));
                return;                              // don't pass up

            // GemStoneAddition - passed up from TCPGOSSIP if none of
            // the locators has a distributed system
            case Event.ENABLE_INITIAL_COORDINATOR:
              disable_initial_coord = false;
              return;
              
            // GemStoneAddiiton - network partitioning detection
            case Event.FLOATING_COORDINATOR_DISABLED:
              boolean warning = true;
              if (!floatingCoordinatorDisabled) {
                floatingCoordinatorDisabled = true;
                log.getLogWriter().info(
                    ExternalStrings.GMS_LOCATOR_HAS_DISABLED_FLOATING_MEMBERSHIP_COORDINATION);
              } else {
                warning = false;
              }
              if (!stack.gfPeerFunctions.hasLocator()) {
                if (warning) {
                  log.getLogWriter().fine("This member of the distributed system will only be a coordinator if there are no locators available");
                }
                ((IpAddress)local_addr).shouldntBeCoordinator(true);
                Event newaddr = new Event(Event.SET_LOCAL_ADDRESS,
                    this.local_addr);
                passUp(newaddr);
                passDown(newaddr);
              }
              else if (warning && log.getLogWriter().fineEnabled()) {
                log.getLogWriter().fine(
                    "This VM hosts a locator and is preferred as membership coordinator.");
              }
              return;
              
            case Event.ENABLE_NETWORK_PARTITION_DETECTION:
              this.splitBrainDetectionEnabled = true;
              this.stack.gfPeerFunctions.enableNetworkPartitionDetection();
              return;

            // GemStoneAddition - copied from receiveUpEvent for removal of threading model
            case Event.GET_DIGEST_OK:
              digest_promise.setResult(evt.getArg());
              return; // don't pass further up

        }

        if(impl.handleUpEvent(evt))
            passUp(evt);
    }
    
    /**
     This method is overridden to avoid hanging on getDigest(): when a JOIN is received, the coordinator needs
     to retrieve the digest from the NAKACK layer. It therefore sends down a GET_DIGEST event, to which the NAKACK layer
     responds with a GET_DIGEST_OK event.<p>
     However, the GET_DIGEST_OK event will not be processed because the thread handling the JOIN request won't process
     the GET_DIGEST_OK event until the JOIN event returns. The receiveUpEvent() method is executed by the up-handler
     thread of the lower protocol and therefore can handle the event. All we do here is unblock the mutex on which
     JOIN is waiting, allowing JOIN to return with a valid digest. The GET_DIGEST_OK event is then discarded, because
     it won't be processed twice.
     */
//    @Override // GemStoneAddition  
//    public void receiveUpEvent(Event evt) {
//        switch(evt.getType()) {
//            case Event.GET_DIGEST_OK:
//                digest_promise.setResult(evt.getArg());
//                return; // don't pass further up
//        }
//        super.receiveUpEvent(evt);
//    }


    @Override // GemStoneAddition  
    public void down(Event evt) {
        switch(evt.getType()) {

            case Event.CONNECT:
                RuntimeException joinException = null; // GemStoneAddition
                passDown(evt);
                if(local_addr == null)
                    if(log.isFatalEnabled()) log.fatal("[CONNECT] local_addr is null");
                joined = false;  // GemStoneAddition
                try {
                  joined = impl.join(local_addr);
                }
                catch (ShunnedAddressException e) { // GemStoneAddition 
                  joinException = e;
                }
                catch (RuntimeException e) {
                  String exClass = e.getClass().getSimpleName();
                  if (exClass.equals("SystemConnectException")
                      || exClass.equals("AuthenticationFailedException")
                      || exClass.equals("GemFireConfigException")) { // GemStoneAddition
                    joinException = e;
                  } else {
                    throw e;
                  }
                }
                if (trace)
                  log.trace("GMS join returned " + joined);
                if (joined) {
                  // GemStoneAddition - return status from impl
                    Event OK = new Event(Event.CONNECT_OK);
                    passUp(OK);
                    // GemStoneAddition - pass down as well, so lower protocols know we're connected
                    passDown(OK);
                }
                else {
                    if (joinException == null) { // GemStoneAddition
                      joinException = stack.gfBasicFunctions.getSystemConnectException("Attempt to connect to distributed system timed out");
                    }
                    if (log.getLogWriter().fineEnabled()) {
                      log.getLogWriter().fine("Startup is throwing " + joinException);
                    }
                    passUp(new Event(Event.EXIT, joinException));
                }
                if (trace)
                  log.trace("GMS connect completed");
                return;                              // don't pass down: was already passed down

            case Event.DISCONNECT:
                Event disconnecting = new Event(Event.DISCONNECTING); // GemStoneAddition - starting to disconnect
                passUp(disconnecting);
                passDown(disconnecting);
                impl.leave((Address)evt.getArg());
                this.disconnected = true;
                passUp(new Event(Event.DISCONNECT_OK));
                // bug #44786 - GemFire does not issue connect() after disconnecting a stack.  Nulling out the view
                // variable causes an NPE in the view casting thread if a view is being prepared during shutdown and
                // can cause NPEs in other places that do not have null checks for this variable
                //initState(); // in case connect() is called again
                break;       // pass down
        }

//        boolean handled = impl.handleDownEvent(evt);  GemStoneAddition - this does nothing but slow things down a bit
//        if(handled)
            passDown(evt);
    }


    /** Setup the Protocol instance according to the configuration string */
    @Override // GemStoneAddition  
    public boolean setProperties(Properties props) {
        String str;

        super.setProperties(props);
        str=props.getProperty("shun");
        if(str != null) {
            shun=Boolean.valueOf(str).booleanValue();
            props.remove("shun");
        }

        str=props.getProperty("merge_leader");
        if(str != null) {
            merge_leader=Boolean.valueOf(str).booleanValue();
            props.remove("merge_leader");
        }

        str=props.getProperty("print_local_addr");
        if(str != null) {
            print_local_addr=Boolean.valueOf(str).booleanValue();
            props.remove("print_local_addr");
        }

        str=props.getProperty("join_timeout");           // time to wait for JOIN
        if(str != null) {
            join_timeout=Long.parseLong(str);
            props.remove("join_timeout");
        }

        str=props.getProperty("join_retry_timeout");     // time to wait between JOINs
        if(str != null) {
            join_retry_timeout=Long.parseLong(str);
            props.remove("join_retry_timeout");
        }

        str=props.getProperty("leave_timeout");           // time to wait until coord responds to LEAVE req.
        if(str != null) {
            leave_timeout=Long.parseLong(str);
            props.remove("leave_timeout");
        }

        str=props.getProperty("merge_timeout");           // time to wait for MERGE_RSPS from subgroup coordinators
        if(str != null) {
            merge_timeout=Long.parseLong(str);
            props.remove("merge_timeout");
        }

        str=props.getProperty("digest_timeout");          // time to wait for GET_DIGEST_OK from PBCAST
        if(str != null) {
            digest_timeout=Long.parseLong(str);
            props.remove("digest_timeout");
        }

        str=props.getProperty("view_ack_collection_timeout");
        if(str != null) {
            view_ack_collection_timeout=Long.parseLong(str);
            props.remove("view_ack_collection_timeout");
        }

        str=props.getProperty("resume_task_timeout");
        if(str != null) {
            resume_task_timeout=Long.parseLong(str);
            props.remove("resume_task_timeout");
        }

        str=props.getProperty("disable_initial_coord");
        if(str != null) {
            disable_initial_coord=Boolean.valueOf(str).booleanValue();
            props.remove("disable_initial_coord");
        }
        // GemStoneAddition - InternalLocator needs this
        if (Boolean.getBoolean("p2p.enableInitialCoordinator")) {
          disable_initial_coord = false;
        }
        
        //GemStoneAddition - split-brain detection support
        str=props.getProperty("split-brain-detection");
        if (str != null) {
          splitBrainDetectionEnabled = Boolean.valueOf(str).booleanValue();
          props.remove("split-brain-detection");
        }
        str=props.getProperty("partition-threshold");
        int l = 51;
        if (str != null) {
          l = Integer.parseInt(str);
          props.remove("partition-threshold");
        }
        this.partitionThreshold = l;

        str=props.getProperty("member-weight");
        l = 0;
        if (str != null) {
          l = Integer.parseInt(str);
          props.remove("member-weight");
        }
        this.memberWeight = l;

        str=props.getProperty("handle_concurrent_startup");
        if(str != null) {
            handle_concurrent_startup=Boolean.valueOf(str).booleanValue();
            props.remove("handle_concurrent_startup");
        }

        str=props.getProperty("num_prev_mbrs");
        if(str != null) {
            num_prev_mbrs=Integer.parseInt(str);
            props.remove("num_prev_mbrs");
        }

        if(props.size() > 0) {
            log.error(ExternalStrings.GMS_GMSSETPROPERTIES_THE_FOLLOWING_PROPERTIES_ARE_NOT_RECOGNIZED__0, props);

            return false;
        }
        return true;
    }



    /* ------------------------------- Private Methods --------------------------------- */

    void initState() {
        becomeClient();
        view_id=null;
        view=null;
        leader = null; // GemStoneAddition
    }
    
    /**
     * GemStoneAddition - notify interested parties that we've selected an initial
     * coordinator and joined the group
     */
    protected void notifyOfCoordinator(Address coord) {
      GossipServer gs = GossipServer.getInstance();
      if (gs != null) {
        gs.setCoordinator(coord);
      }
    }

    /**
     * GemStoneAddition - notify interested parties that we've selected an initial
     * coordinator and joined the group
     */
    protected void notifyOfLocalAddress(Address localAddress) {
      GossipServer gs = GossipServer.getInstance();
      if (gs != null) {
        gs.setLocalAddress(localAddress);
      }
    }

    /* --------------------------- End of Private Methods ------------------------------- */



    public static class GmsHeader extends Header implements Streamable {
        public static final byte JOIN_REQ=1;
        public static final byte JOIN_RSP=2;
        public static final byte LEAVE_REQ=3;
        public static final byte LEAVE_RSP=4;
        public static final byte VIEW=5;
        public static final byte MERGE_REQ=6;
        public static final byte MERGE_RSP=7;
        public static final byte INSTALL_MERGE_VIEW=8;
        public static final byte CANCEL_MERGE=9;
        public static final byte VIEW_ACK=10;
        public static final byte GET_VIEW=11; // GemStoneAddition
        public static final byte GET_VIEW_RSP=12; // GemStoneAddition
        public static final byte REMOVE_REQ=13; // GemStoneAddition - member removal req
        public static final byte PREPARE_FOR_VIEW=14; // GemStoneAddition - network partition detection
        public static final byte PREPARE_FOR_VIEW_ACK=15; // GemStoneAddition - network partition detection
        public static final byte NETWORK_PARTITION_DETECTED=16; // GemStoneAddition - network partition detection

        byte type=0;
//        private View view=null;       // used when type=VIEW or MERGE_RSP or INSTALL_MERGE_VIEW
        Address mbr=null;             // used when type=JOIN_REQ or LEAVE_REQ
//        JoinRsp join_rsp=null;        // used when type=JOIN_RSP
//        Digest my_digest=null;          // used when type=MERGE_RSP or INSTALL_MERGE_VIEW
        ViewId merge_id=null;        // used when type=MERGE_REQ or MERGE_RSP or INSTALL_MERGE_VIEW or CANCEL_MERGE
        boolean merge_rejected=false; // used when type=MERGE_RSP
        String arg = ""; // GemStoneAddition
        boolean forcedOut; // GemStoneAddition for LEAVE_RSP after REMOVE_REQ


        public GmsHeader() {
        } // used for Externalization

        public GmsHeader(byte type) {
            this.type=type;
        }


        /** Used for VIEW header */
//        public GmsHeader(byte type, View view) {
//            this.type=type;
//            this.view=view;
//        }


        /** Used for JOIN_REQ or LEAVE_REQ header */
        public GmsHeader(byte type, Address mbr) {
            this.type=type;
            this.mbr=mbr;
        }

        /** GemStoneAddition - for REMOVE_REQ */
        public GmsHeader(byte type, Address mbr, String reason) {
          this.type=type;
          this.mbr=mbr;
          this.arg = reason;
        }
        
        /** GemStoneAddition for LEAVE_RSP sent from a REMOVE_REQ */
        public GmsHeader(byte type, boolean forcedOut, String reason, Address mbr) {
          this.type = type;
          this.forcedOut = forcedOut;
          this.arg = reason;
          this.mbr = mbr;
        }

//        /** Used for JOIN_RSP header */
//        public GmsHeader(byte type, JoinRsp join_rsp) {
//            this.type=type;
//            this.join_rsp=join_rsp;
//        }

        public byte getType() {
            return type;
        }

        public Address getMember() {
            return mbr;
        }
        
        public String getArg() { // GemStoneAddition
          return this.arg;
        }

        // GemStoneAddition
//        public View getView() {
//            return view;
//        }

        @Override // GemStoneAddition  
        public String toString() {
            StringBuffer sb=new StringBuffer("GmsHeader");
            sb.append('[' + type2String(type) + ']');
            switch(type) {
                case JOIN_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case JOIN_RSP:
                case GET_VIEW_RSP: // GemStoneAddition - for becomeGroupCoordinator()
//                    sb.append(": join_rsp=" + join_rsp);
                    break;

                case LEAVE_REQ:
                    sb.append(": mbr=" + mbr);
                    break;

                case LEAVE_RSP:
                    // GemStoneAddition - forcedOut & arg
                    if (forcedOut) {
                      sb.append(": forced removal because ").append(this.arg);
                    }
                    break;

                case VIEW:
                case VIEW_ACK:
//                    sb.append(": view=" + view);
                    break;

                case MERGE_REQ:
                    sb.append(": merge_id=" + merge_id);
                    break;

                case MERGE_RSP:
                    sb.append(/*": view=" + view + " digest=" + my_digest +*/ ", merge_rejected=" + merge_rejected +
                            ", merge_id=" + merge_id);
                    break;

                case INSTALL_MERGE_VIEW:
//                    sb.append(/*": view=" + view + ", digest=" + my_digest*/);
                    break;

                case CANCEL_MERGE:
                    sb.append(", <merge cancelled>, merge_id=" + merge_id);
                    break;
                
                case REMOVE_REQ:
                  sb.append(": mbr=" + mbr + ", arg=" + this.arg); // GemStoneAddition
                  break;
            }
            return sb.toString();
        }


        public static String type2String(int type) {
            switch(type) {
                case JOIN_REQ: return "JOIN_REQ";
                case JOIN_RSP: return "JOIN_RSP";
                case LEAVE_REQ: return "LEAVE_REQ";
                case LEAVE_RSP: return "LEAVE_RSP";
                case VIEW: return "VIEW";
                case MERGE_REQ: return "MERGE_REQ";
                case MERGE_RSP: return "MERGE_RSP";
                case INSTALL_MERGE_VIEW: return "INSTALL_MERGE_VIEW";
                case CANCEL_MERGE: return "CANCEL_MERGE";
                case VIEW_ACK: return "VIEW_ACK";
                case REMOVE_REQ: return "REMOVE_REQ";
                case GET_VIEW: return "GET_VIEW";
                case GET_VIEW_RSP: return "GET_VIEW_RSP";
                case PREPARE_FOR_VIEW: return "PREPARE_FOR_VIEW";
                case PREPARE_FOR_VIEW_ACK: return "PREPARE_FOR_VIEW_ACK";
                case NETWORK_PARTITION_DETECTED: return "NETWORK_PARTITION_DETECTED";
                default: return "<unknown>";
            }
        }


        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeByte(type);
//            out.writeObject(view);
            out.writeObject(mbr);
//            out.writeObject(join_rsp);
//            out.writeObject(my_digest);
            out.writeObject(merge_id);
            out.writeBoolean(merge_rejected);
            out.writeBoolean(this.forcedOut); // GemStoneAddition
            out.writeObject(this.arg); // GemStoneAddition
        }


        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            type=in.readByte();
//            view=(View)in.readObject();
            mbr=(Address)in.readObject();
//            join_rsp=(JoinRsp)in.readObject();
//            my_digest=(Digest)in.readObject();
            merge_id=(ViewId)in.readObject();
            merge_rejected=in.readBoolean();
            this.forcedOut = in.readBoolean(); // GemStoneAddition
            this.arg = (String)in.readObject(); // GemStoneAddition
        }


        public void writeTo(DataOutputStream out) throws IOException {
            out.writeByte(type);
//            boolean isMergeView=view != null && view instanceof MergeView;
//            out.writeBoolean(isMergeView);
//            Util.writeStreamable(view, out);
            Util.writeAddress(mbr, out);
//            Util.writeStreamable(join_rsp, out);
//            Util.writeStreamable(my_digest, out);
            Util.writeStreamable(merge_id, out); // kludge: we know merge_id is a ViewId
            out.writeBoolean(merge_rejected);
            out.writeBoolean(this.forcedOut); // GemStoneAddition
            if (this.arg == null) { // GemStoneAddition arg
              out.writeBoolean(false);
            }
            else {
              out.writeBoolean(true);
              out.writeUTF(this.arg); // GemStoneAddition
            }
        }

        public void readFrom(DataInputStream in) throws IOException, IllegalAccessException, InstantiationException {
            type=in.readByte();
//            boolean isMergeView=in.readBoolean();
//            if(isMergeView)
//                view=(View)Util.readStreamable(MergeView.class, in);
//            else
//                view=(View)Util.readStreamable(View.class, in);
            mbr=Util.readAddress(in);
//            join_rsp=(JoinRsp)Util.readStreamable(JoinRsp.class, in);
//            my_digest=(Digest)Util.readStreamable(Digest.class, in);
            merge_id=(ViewId)Util.readStreamable(ViewId.class, in);
            merge_rejected=in.readBoolean();
            this.forcedOut = in.readBoolean(); // GemStoneAddition
            boolean hasArg = in.readBoolean(); // GemStoneAddition
            if (hasArg) {
              this.arg = in.readUTF(); // GemStoneAddition
            }
        }

        @Override // GemStoneAddition  
        public long size(short version) {
            long retval=Global.BYTE_SIZE *2; // type + merge_rejected

            retval+=Global.BYTE_SIZE; // presence view
            retval+=Global.BYTE_SIZE; // MergeView or View
//            if(view != null)
//                retval+=view.serializedSize(version);

            retval+=Util.size(mbr,version);

//            retval+=Global.BYTE_SIZE; // presence of join_rsp
//            if(join_rsp != null)
//                retval+=join_rsp.serializedSize(version);

//            retval+=Global.BYTE_SIZE; // presence for my_digest
//            if(my_digest != null)
//                retval+=my_digest.serializedSize(version);

            retval+=Global.BYTE_SIZE; // presence for merge_id
            if(merge_id != null)
                retval+=merge_id.serializedSize(version);
            
            if (this.arg != null) {
              retval += this.arg.length(); // GemStoneAddition - arg string
            }
            retval += Global.BYTE_SIZE; // GemStoneAddition - forcedOut
            return retval;
        }

    }




    public static class Request  {
        static final int JOIN    = 1;
        static final int LEAVE   = 2;
        static final int SUSPECT = 3;
        static final int MERGE   = 4;
        static final int VIEW    = 5;


        int     type=-1;
        Address mbr=null;
        boolean suspected;
        Vector  coordinators=null;
        View    view=null;
        Digest  digest=null;
        List    target_members=null;
        String reason; // GemStoneAddition

        Request(int type) {
            this.type=type;
        }

        Request(int type, Address mbr, boolean suspected, Vector coordinators) {
            this.type=type;
            this.mbr=mbr;
            this.suspected=suspected;
            this.coordinators=coordinators;
        }

        /** GemStoneAddition - carry the reason with a leave req */
        Request(int type, Address mbr, boolean suspected, Vector coordinators, String reason) {
          this.type=type;
          this.mbr=mbr;
          this.suspected=suspected;
          this.coordinators=coordinators;
          this.reason = reason;
        }

        @Override // GemStoneAddition  
        public String toString() {
            switch(type) {
                case JOIN:    return "JOIN(" + mbr + ")";
                case LEAVE:   return "LEAVE(" + mbr + ", " + suspected + ")";
                case SUSPECT: return "SUSPECT(" + mbr + ")";
                case MERGE:   return "MERGE(" + coordinators + ")";
                case VIEW:    return "VIEW (" + view.getVid() + ")";
            }
            return "<invalid (type=" + type + ")";
        }
    }




    /**
     * Class which processes JOIN, LEAVE and MERGE requests. Requests are queued and processed in FIFO order
     * @author Bela Ban
     * @version $Id: GMS.java,v 1.49 2005/12/23 14:57:06 belaban Exp $
     */
    class ViewHandler implements Runnable {
        Thread                    viewThread; // GemStoneAddition -- accesses synchronized on this
        com.gemstone.org.jgroups.util.Queue                     q=new com.gemstone.org.jgroups.util.Queue(); // Queue<Request>
        boolean                   suspended=false;
        final static long         INTERVAL=5000;
        private static final long MAX_COMPLETION_TIME=10000;
        /** Maintains a list of the last 20 requests */
        private final BoundedList history=new BoundedList(20);

        /** Map<Object,TimeScheduler.CancellableTask>. Keeps track of Resumer tasks which have not fired yet */
        private final Map         resume_tasks=new HashMap();
        private Object            merge_id=null;


        void add(Request req) {
            add(req, false, false);
        }

        synchronized void add(Request req, boolean at_head, boolean unsuspend) {
            if(suspended && !unsuspend) {
                log.warn("queue is suspended; request " + req + " is discarded");
                return;
            }
            start(unsuspend);
            try {
                if(at_head)
                    q.addAtHead(req);
                else
                    q.add(req);
                history.add(new Date() + ": " + req.toString());
            }
            catch(QueueClosedException e) {
                if(trace)
                    log.trace("queue is closed; request " + req + " is discarded");
            }
        }


        synchronized void waitUntilCompleted(long timeout) {
            if(viewThread != null) {
                try {
                    viewThread.join(timeout);
                }
                catch(InterruptedException e) {
                  Thread.currentThread().interrupt(); // GemStoneAddition
                }
            }
        }

        /**
         * Waits until the current request has been processes, then clears the queue and discards new
         * requests from now on
         */
        public synchronized void suspend(Object m_id) {
            if(suspended)
                return;
            suspended=true;
            this.merge_id=m_id;
            q.clear();
            waitUntilCompleted(MAX_COMPLETION_TIME);
            q.close(true);
            if(trace)
                log.trace("suspended ViewHandler");
            Resumer r=new Resumer(resume_task_timeout, m_id, resume_tasks, this);
            resume_tasks.put(m_id, r);
            timer.add(r);
        }


        public synchronized void resume(Object m_id) {
            if(!suspended)
                return;
            boolean same_merge_id=this.merge_id != null && m_id != null && this.merge_id.equals(m_id);
            same_merge_id=same_merge_id || (this.merge_id == null && m_id == null);

            if(!same_merge_id) {
                if(warn)
                    log.warn("resume(" +m_id+ ") does not match " + this.merge_id + ", ignoring resume()");
                return;
            }
            synchronized(resume_tasks) {
                TimeScheduler.CancellableTask task=(TimeScheduler.CancellableTask)resume_tasks.get(m_id);
                if(task != null) {
                    task.cancel();
                    resume_tasks.remove(m_id);
                }
            }
            resumeForce();
        }

        public synchronized void resumeForce() {
            if(q.closed())
                q.reset();
            suspended=false;
            if(trace)
                log.trace("resumed ViewHandler");
        }

        public void run() {
            Request req;
            for (;;) { // GemStoneAddition - remove coding anti-pattern
              if (q.closed()) break; // GemStoneAddition
              if (Thread.currentThread().isInterrupted()) break; // GemStoneAddition
                try {
                    req=(Request)q.remove(INTERVAL); // throws a TimeoutException if it runs into timeout
                    process(req);
                    if (Thread.currentThread().isInterrupted()) { // GemStoneAddition
                      return;
                    }
                }
                catch(QueueClosedException e) {
                    break;
                }
                catch(TimeoutException e) {
//                  if (trace) {
//                    log.trace("ViewHandler queue timeout - retrying");
//                  }
                  continue;
                    //break; GemStoneAddition: fix for bug #42009.  don't exit the view handler so hastily
                }
            }
            if (trace) {
              log.trace("ViewHandler is exiting");
            }
        }

        public int size() {
          return q.size();
        }
        public boolean suspended() {return suspended;}
        public String dumpQueue() {
            StringBuffer sb=new StringBuffer();
            List v=q.values();
            for(Iterator it=v.iterator(); it.hasNext();) {
                sb.append(it.next() + "\n");
            }
            return sb.toString();
        }

        public String dumpHistory() {
            StringBuffer sb=new StringBuffer();
            for(Enumeration en=history.elements(); en.hasMoreElements();) {
                sb.append(en.nextElement() + "\n");
            }
            return sb.toString();
        }

        private void process(Request reqp) {
//log.getLogWriter().info("gms viewhandler processing " + req); // debugging
            List joinReqs = new ArrayList();
            List leaveReqs = new ArrayList();
            List suspectReqs = new ArrayList();
            List suspectReasons = new ArrayList();
            boolean loop = true;
            long waittime = BUNDLE_WAITTIME; // time to wait for additional JOINs
            long starttime = System.currentTimeMillis();
            Request req = reqp;
            while (loop) {
              if(trace)
                log.trace("processing " + req);
              switch(req.type) {
                case Request.JOIN: {
                    if (!joinReqs.contains(req.mbr)) { // GemStoneAddition - duplicate check
                      joinReqs.add(req.mbr);
                    }
//                    impl.handleJoin(joinReqs);
                    break;
                }
                case Request.LEAVE:
                    if(req.suspected) {
//                        impl.suspect(req.mbr, req.reason);
                      if (!suspectReqs.contains(req.mbr)) {
                        suspectReqs.add(req.mbr);
                        suspectReasons.add(req.reason);
                      }
                    }
                    else {
                      if (!leaveReqs.contains(req.mbr)) {
                        leaveReqs.add(req.mbr);
                      }
                    }
                    break;
//                case Request.SUSPECT:  GemStoneAddition: this is no longer used
//                    impl.suspect(req.mbr);
//                    loop = false;
//                    break;
                case Request.MERGE:
                    impl.merge(req.coordinators);
                    loop = false;
                    break;
                case Request.VIEW:
                    boolean mcast = stack.gfPeerFunctions.getMcastPort() > 0; // GemStoneAddition
                    castViewChangeWithDest(req.view, req.digest, req.target_members, mcast);
                    loop = false;
                    break;
                default:
                    log.error(ExternalStrings.GMS_REQUEST__0__IS_UNKNOWN_DISCARDED, req.type);
                    loop = false;
              }
              if (waittime <= 0) {
                loop = false;
              }
              if (loop) {
                boolean ignoreTimeout = false;
                try {
                  Request addl = (Request)q.peek(waittime);
                  if (addl.type == Request.JOIN || addl.type == Request.LEAVE) {
                    try {
                      req = (Request)q.remove();
                      ignoreTimeout = true;
                    }
                    catch (InterruptedException e) {
                      // Thread.interrupt();
                      
                      // Disregard this.  We are only interrupted at
                      // time of closing, at which point the queue
                      // has been closed and flushed.  We process
                      // the remaining messages and then quit....
                    }
                  }
                }
                catch (TimeoutException e) {
                  // no more events.  Finish processing.
                  loop = ignoreTimeout;
                }
                catch (QueueClosedException e) {
                  // We've been stopped.
                  loop = ignoreTimeout;
                }
                waittime -= (System.currentTimeMillis() - starttime);
                if (!ignoreTimeout && waittime <= 0) {
                  loop = false;
                }
              } // if (loop)
            } // while (loop)
            if (!joinReqs.isEmpty() || !leaveReqs.isEmpty() || !suspectReqs.isEmpty()) {
              impl.handleJoinsAndLeaves(joinReqs, leaveReqs, suspectReqs, suspectReasons, false/*forceInclusion*/);
            }
        }

        synchronized void start(boolean unsuspend) {
            if(q.closed())
                q.reset();
            if(unsuspend) {
                suspended=false;
                synchronized(resume_tasks) {
                    TimeScheduler.CancellableTask task=(TimeScheduler.CancellableTask)resume_tasks.get(merge_id);
                    if(task != null) {
                        task.cancel();
                        resume_tasks.remove(merge_id);
                    }
                }
            }
            merge_id=null;
            if(viewThread == null || !viewThread.isAlive()) {
                viewThread=new Thread(GemFireTracer.GROUP, this, "ViewHandler");
                viewThread.setDaemon(true);
                viewThread.start();
                if(trace)
                    log.trace("ViewHandler started");
            }
        }

        synchronized void stop(boolean flush) {
            q.close(flush);
            if (viewThread != null && viewThread.isAlive()) viewThread.interrupt(); // GemStoneAddition
            viewThread = null; // GemStoneAddition
            TimeScheduler.CancellableTask task;
            synchronized(resume_tasks) {
                for(Iterator it=resume_tasks.values().iterator(); it.hasNext();) {
                    task=(TimeScheduler.CancellableTask)it.next();
                    task.cancel();
                }
                resume_tasks.clear();
            }
            merge_id=null;
            resumeForce();
        }
    }


    /**
     * Resumer is a second line of defense: when the ViewHandler is suspended, it will be resumed when the current
     * merge is cancelled, or when the merge completes. However, in a case where this never happens (this
     * shouldn't be the case !), the Resumer will nevertheless resume the ViewHandler.
     * We chose this strategy because ViewHandler is critical: if it is suspended indefinitely, we would
     * not be able to process new JOIN requests ! So, this is for peace of mind, although it most likely
     * will never be used...
     */
    static class Resumer implements TimeScheduler.CancellableTask {
        boolean           cancelled=false;
        long              interval;
        final Object      token;
        final Map         tasks;
        final ViewHandler handler;


        public Resumer(long interval, final Object token, final Map t, final ViewHandler handler) {
            this.interval=interval;
            this.token=token;
            this.tasks=t;
            this.handler=handler;
        }

        public void cancel() {
            cancelled=true;
        }

        public boolean cancelled() {
            return cancelled;
        }

        public long nextInterval() {
            return interval;
        }

        public void run() {
            TimeScheduler.CancellableTask t;
            boolean execute=true;
            synchronized(tasks) {
                t=(TimeScheduler.CancellableTask)tasks.get(token);
                if(t != null) {
                    t.cancel();
                    execute=true;
                }
                else {
                    execute=false;
                }
                tasks.remove(token);
            }
            if(execute) {
                handler.resume(token);
            }
        }
    }
    
    


}
