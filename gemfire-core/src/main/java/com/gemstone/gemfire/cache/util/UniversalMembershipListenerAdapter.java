/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.util;

import com.gemstone.gemfire.admin.AdminDistributedSystem;
import com.gemstone.gemfire.admin.SystemMembershipEvent;
import com.gemstone.gemfire.admin.SystemMembershipListener;
import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.Assert;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

import java.util.*;

/**
 * <p>The <code>UniversalMembershipListenerAdapter</code> is a wrapper 
 * for {@link com.gemstone.gemfire.admin.SystemMembershipListener} and 
 * {@link BridgeMembershipListener}, providing a facade that makes both
 * appear to customer code as a single <code>SystemMembershipListener</code>
 * from the Admin API. This includes adapting 
 * <code>BridgeMembershipListener</code> events to appear as events for the 
 * <code>SystemMembershipListener</code>.</p>
 *
 * <p><code>UniversalMembershipListenerAdapter</code> implements
 * <code>SystemMembershipListener</code>, exposing the callbacks in that
 * interface as methods to be overridden by the customer.</p>
 *
 * <p>An internal implementation of <code>BridgeMembershipListener</code> is 
 * registered when this class is instantiated. This implementation creates a
 * {@link com.gemstone.gemfire.admin.SystemMembershipEvent} and calls the
 * corresponding <code>SystemMembershipListener</code> public methods on 
 * <code>UniversalMembershipListenerAdapter</code>. To the customer code, the
 * <code>BridgeMembershipEvent</code>s are wrapped to appear as 
 * <code>SystemMembershipEvent</code>s. In this way, both types of membership
 * events appear as <code>SystemMembershipEvent</code>s, allowing customer
 * code written using the Admin API to continue working by changing the
 * listener implementation to simply extend this class.</p>
 *
 * <p>Any BridgeServer using the <code>UniversalMembershipListenerAdapter</code>
 * will receive notifications of system membership changes and bridge
 * membership changes through a single listener.</p>
 *
 * <p>Any bridge client using the <code>UniversalMembershipListenerAdapter</code>
 * would receive notifications of bridge server connection changes. If that
 * bridge client also creates a connection to the GemFire {@link 
 * com.gemstone.gemfire.distributed.DistributedSystem}, then it will also
 * receive notifications of system membership changes.</p>
 *
 * <p>Subclasses of <code>UniversalMembershipListenerAdapter</code> may be
 * registered as a <code>SystemMembershipListener</code> using {@link 
 * com.gemstone.gemfire.admin.AdminDistributedSystem#addMembershipListener}.
 * It is best, however, to register the listener using {@link
 * #registerMembershipListener} since this allows the adapter to prevent
 * duplicate events for members that are both a system member and a bridge
 * member.</p>
 *
 * <p>Simply constructing the <code>UniversalMembershipListenerAdapter</code>
 * results in the underlying <code>BridgeMembershipListener</code> also being
 * registered.</p>
 *
 * <p>The following code illustrates how a BridgeServer application would use
 * <code>UniversalMembershipListenerAdapter</code>. The code in this example
 * assumes that the class MyMembershipListenerImpl extends 
 * <code>UniversalMembershipListenerAdapter</code>:
 * <pre><code>
 * public class MyMembershipListenerImpl extends UniversalMembershipListenerAdapter {
 *   public void memberCrashed(SystemMembershipEvent event) {
 *     // customer code
 *   }
 *   public void memberLeft(SystemMembershipEvent event) {
 *     // customer code
 *   }
 *   public void memberJoined(SystemMembershipEvent event) {
 *     // customer code
 *   }
 * }
 *
 * DistributedSystemConfig config = 
 *   AdminDistributedSystemFactory.defineDistributedSystem(myDS, null);
 * AdminDistributedSystem adminDS = 
 *   AdminDistributedSystemFactory.getDistributedSystem(config);
 * adminDS.connect();
 * MyMembershipListenerImpl myListener = new MyMembershipListenerImpl();
 * myListener.registerMembershipListener(adminDS);
 * </code></pre>
 * The callbacks on MyMembershipListenerImpl would then be
 * invoked for all <code>SystemMembershipEvent</code>s and 
 * <code>BridgeMembershipEvent</code>s. The latter will appear to be 
 * <code>SystemMembershipEvent</code>s.</p>
 *
 * <p>Similarly, the following code illustrates how a bridge client application
 * would use <code>UniversalMembershipListenerAdapter</code>, where 
 * MyMembershipListenerImpl is a subclass. Simply by constructing this subclass
 * of <code>UniversalMembershipListenerAdapter</code> it is registering itself
 * as a <code>BridgeMembershipListener</code>:
 * <pre><code>
 * new MyMembershipListenerImpl();
 * </code></pre>
 * A bridge client that also connects to the <code>DistributedSystem</code>
 * could register with the<code>AdminDistributedSystem</code> as shown 
 * above.</p>
 *
 * <p>It is recommended that subclasses register with the 
 * <code>AdminDistributedSystem</code> using {@link 
 * #registerMembershipListener}, as this will prevent duplicate events for
 * members that are both bridge members and system members. If duplicate
 * events are acceptable, you may register subclasses using {@link 
 * com.gemstone.gemfire.admin.AdminDistributedSystem#addMembershipListener 
 * AdminDistributedSystem#addMembershipListener}.</p>
 *
 * @author Kirk Lund
 * @since 4.2.1
 * @deprecated Use com.gemstone.gemfire.management.membership.UniversalMembershipListenerAdapter instead.
 */
public abstract class UniversalMembershipListenerAdapter 
implements SystemMembershipListener {
  
  /** 
   * Default number of historical events to track in order to avoid duplicate
   * events for members that are both bridge members and system members;
   * value is 100.
   */
  public static final int DEFAULT_HISTORY_SIZE = 100;
  
//  private final Object[] eventHistory;
//  private final boolean[] eventJoined;
//  private boolean registered = false;
  
  protected final int historySize;
  protected final LinkedList<String> eventHistory; // list of String memberIds
  protected final Map<String,Boolean> eventJoined; // key: memberId, value: Boolean
  
  // TODO: perhaps ctor should require AdminDistributedSystem as arg?
  
  /** Constructs an instance of UniversalMembershipListenerAdapter. */
  public UniversalMembershipListenerAdapter() {
    this(DEFAULT_HISTORY_SIZE);
  }
  
  /** 
   * Constructs an instance of UniversalMembershipListenerAdapter.
   * @param historySize number of historical events to track in order to avoid
   * duplicate events for members that are both bridge members and system
   * members; must a number between 10 and <code>Integer.MAX_INT</code>
   * @throws IllegalArgumentException if historySizde is less than 10
   */
  public UniversalMembershipListenerAdapter(int historySize) {
    if (historySize < 10) {
      throw new IllegalArgumentException(LocalizedStrings.UniversalMembershipListenerAdapter_ARGUMENT_HISTORYSIZE_MUST_BE_BETWEEN_10_AND_INTEGERMAX_INT_0.toLocalizedString(Integer.valueOf(historySize)));
    }
    this.historySize = historySize;
    this.eventHistory = new LinkedList<String>();
    this.eventJoined = new HashMap<String,Boolean>();
    BridgeMembership.registerBridgeMembershipListener(this.bridgeMembershipListener);
  }
  
  /**
   * Registers this adapter with the <code>AdminDistributedSystem</code>. 
   * Registering in this way allows the adapter to ensure that callbacks will
   * not be invoked twice for members that have a bridge connection and a
   * system connection. If you register with {@link
   * com.gemstone.gemfire.admin.AdminDistributedSystem#addMembershipListener}
   * then duplicate events may occur for members that are both bridge members
   * and system.
   */
  public void registerMembershipListener(AdminDistributedSystem admin) {
    synchronized (this.eventHistory) {
//      this.registered = true;
      admin.addMembershipListener(this.systemMembershipListener);
    }
  }

  /**
   * Unregisters this adapter with the <code>AdminDistributedSystem</code>. 
   * If registration is performed with {@link #registerMembershipListener}
   * then this method must be used to successfuly unregister the adapter.
   */
  public void unregisterMembershipListener(AdminDistributedSystem admin) {
    synchronized (this.eventHistory) {
//      this.registered = false;
      admin.removeMembershipListener(this.systemMembershipListener);
    }
    unregisterBridgeMembershipListener();
  }
  
  /**
   * Registers this adapter as a <code>BridgeMembershipListener</code>.
   * Registration is automatic when constructing this adapter, so this call
   * is no necessary unless it was previously unregistered by calling
   * {@link #unregisterBridgeMembershipListener}.
   */
  public void registerBridgeMembershipListener() {
    BridgeMembership.registerBridgeMembershipListener(this.bridgeMembershipListener);
  }
  
  /**
   * Unregisters this adapter as a <code>BridgeMembershipListener</code>.
   * @see #registerBridgeMembershipListener
   */
  public void unregisterBridgeMembershipListener() {
    BridgeMembership.unregisterBridgeMembershipListener(this.bridgeMembershipListener);
  }
  
  /**
   * Invoked when a member has joined the distributed system. Also invoked when
   * a client has connected to this process or when this process has connected
   * to a <code>BridgeServer</code>.
   */
  public void memberJoined(SystemMembershipEvent event) {}

  /**
   * Invoked when a member has gracefully left the distributed system. Also
   * invoked when a client has gracefully disconnected from this process.
   * or when this process has gracefully disconnected from a 
   * <code>BridgeServer</code>.   */
  public void memberLeft(SystemMembershipEvent event) {}

  /**
   * Invoked when a member has unexpectedly left the distributed system. Also
   * invoked when a client has unexpectedly disconnected from this process
   * or when this process has unexpectedly disconnected from a 
   * <code>BridgeServer</code>.
   */
  public void memberCrashed(SystemMembershipEvent event) {}
  
  /** Adapts BridgeMembershipEvent to look like a SystemMembershipEvent */
  public static class AdaptedMembershipEvent implements SystemMembershipEvent {
    private final BridgeMembershipEvent event;

    protected AdaptedMembershipEvent(BridgeMembershipEvent event) {
      this.event = event;
    }
    /**
     * Returns true if the member is a bridge client to a BridgeServer hosted
     * by this process. Returns false if the member is a BridgeServer that this
     * process is connected to.
     */
    public boolean isClient() {
      return event.isClient();
    }

    public String getMemberId() {
      return event.getMemberId();
    }
    
    public DistributedMember getDistributedMember() {
      return event.getMember();
    }
    
    @Override
    public boolean equals(Object other) {
      if (other == this) return true;
      if (other == null) return false;
      if (!(other instanceof AdaptedMembershipEvent)) return  false;
      final AdaptedMembershipEvent that = (AdaptedMembershipEvent) other;
  
      if (this.event != that.event &&
          !(this.event != null &&
          this.event.equals(that.event))) return false;
  
      return true;
    }

    @Override
    public int hashCode() {
      return this.event.hashCode();
    }

    @Override
    public String toString() {
      final StringBuffer sb = new StringBuffer("[AdaptedMembershipEvent: ");
      sb.append(this.event);
      sb.append("]");
      return sb.toString();
    }
  }

  private final BridgeMembershipListener bridgeMembershipListener =
  new BridgeMembershipListener() {
    public void memberJoined(BridgeMembershipEvent event) {
      systemMembershipListener.memberJoined(new AdaptedMembershipEvent(event));
    }
    public void memberLeft(BridgeMembershipEvent event) {
      systemMembershipListener.memberLeft(new AdaptedMembershipEvent(event));
    }
    public void memberCrashed(BridgeMembershipEvent event) {
      systemMembershipListener.memberCrashed(new AdaptedMembershipEvent(event));
    }
  };
  
  protected final SystemMembershipListener systemMembershipListener =
  new SystemMembershipListener() {
    public void memberJoined(SystemMembershipEvent event) {
      if (!isDuplicate(event, true)) {
        UniversalMembershipListenerAdapter.this.memberJoined(event);
      }
    }
    public void memberLeft(SystemMembershipEvent event) {
      if (!isDuplicate(event, false)) {
        UniversalMembershipListenerAdapter.this.memberLeft(event);
      }
    }
    public void memberCrashed(SystemMembershipEvent event) {
      if (!isDuplicate(event, false)) {
        UniversalMembershipListenerAdapter.this.memberCrashed(event);
      }
    }
    protected boolean isDuplicate(SystemMembershipEvent event, boolean joined) {
      synchronized (eventHistory) {
        boolean duplicate = false;
        String memberId = event.getMemberId();
        
        // find memberId in eventHistory...
        int indexOf = eventHistory.indexOf(memberId);
        if (indexOf > -1) {
          // found an event for this member
          if ((eventJoined.get(memberId)).booleanValue() == joined) {
            // we already recorded a matching event for this member
            duplicate = true;
          }
          else {
            // remove the event from history and map... will be re-inserted
            Assert.assertTrue(eventHistory.remove(memberId),
              "Failed to replace entry in eventHistory for " + memberId);
            Assert.assertTrue(eventJoined.remove(memberId) != null,
              "Failed to replace entry in eventJoined for " + memberId);
          }
        }
        
        if (!duplicate) {
          // add the event to the history and map
          if (eventHistory.size() == historySize) {
            // filled the eventHistory, so need to remove first entry
            eventHistory.removeFirst();
          }
          eventHistory.addLast(memberId); // linked list
          eventJoined.put(memberId, Boolean.valueOf(joined)); // boolean map
          Assert.assertTrue(eventHistory.size() <= historySize,
            "Attempted to grow eventHistory beyond maximum of " + historySize);
        }
        return duplicate;
      } // sync
    }
  };
  
}

