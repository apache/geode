/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.management.membership;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.management.ManagementService;

/**
 * <p>
 * The <code>UniversalMembershipListenerAdapter</code> is a wrapper for
 * {@link org.apache.geode.management.membership.MembershipListener} and
 * {@link ClientMembershipListener}, providing a facade that makes both appear as a single
 * <code>MembershipListener</code> . This includes adapting <code>ClientMembershipListener</code>
 * events to appear as events for the <code>MembershipListener</code>.
 * </p>
 *
 * <p>
 * <code>UniversalMembershipListenerAdapter</code> implements <code>MembershipListener</code>,
 * exposing the callback in that interface as methods to be overridden by implementing classes.
 * </p>
 *
 * <p>
 * An internal implementation of <code>ClientMembershipListener</code> is registered when this class
 * is instantiated. This implementation creates a
 * {@link org.apache.geode.management.membership.MembershipEvent} and calls the corresponding
 * <code>MembershipListener</code> public methods on
 * <code>UniversalMembershipListenerAdapter</code>.The <code>ClientMembershipEvent</code>s are
 * wrapped to appear as <code>MembershipEvent</code>s. In this way, both types of membership events
 * appear as <code>MembershipEvent</code>s.
 * </p>
 *
 * <p>
 * Any CacheServer using the <code>UniversalMembershipListenerAdapter</code> will receive
 * notifications of peer membership changes and client membership changes through a single listener.
 * </p>
 *
 * <p>
 * Any cache client using the <code>UniversalMembershipListenerAdapter</code> would receive
 * notifications of cache server connection changes. If that cache client also creates a connection
 * to the GemFire {@link org.apache.geode.distributed.DistributedSystem}, then it will also register
 * the adapter for membership events. But it wont be an automatic process. User needs to register
 * the UniversalMembershipListenerAdapter with ManagementService to receive membership events. How
 * to register UniversalMembershipListenerAdapter with ManagementService is explained below.
 * </p>
 *
 * <p>
 * Subclasses of <code>UniversalMembershipListenerAdapter</code> may be registered as a
 * <code>MembershipListener</code> using
 * {@link org.apache.geode.management.ManagementService#addMembershipListener} .It is best, however,
 * to register the listener using {@link #registerMembershipListener} since this allows the adapter
 * to prevent duplicate events for members that are both a peer member and a client.
 * </p>
 *
 * <p>
 * Simply constructing the <code>UniversalMembershipListenerAdapter</code> results in the underlying
 * <code>ClientMembershipListener</code> also being registered.
 * </p>
 *
 * <p>
 * The following code illustrates how a CacheServer application would use
 * <code>UniversalMembershipListenerAdapter</code>. The code in this example assumes that the class
 * MyMembershipListenerImpl extends <code>UniversalMembershipListenerAdapter</code>:
 *
 * <pre>
 * <code>
 * public class MyMembershipListenerImpl extends UniversalMembershipListenerAdapter {
 *   public void memberCrashed(MembershipEvent event) {
 *     // customer code
 *   }
 *   public void memberLeft(MembershipEvent event) {
 *     // customer code
 *   }
 *   public void memberJoined(MembershipEvent event) {
 *     // customer code
 *   }
 * }
 *
 *  Cache cache = //Get hold of GemFire Cache instance
 *  ManagementService service = ManagementService.getExistingManagementService(cache);
 *
 *  MyMembershipListenerImpl myListener = new MyMembershipListenerImpl();
 *  myListener.registerMembershipListener(service);
 * </code>
 * </pre>
 *
 * The callback on MyMembershipListenerImpl would then be invoked for all
 * <code>MembershipEvent</code>s and <code>ClientMembershipEvent</code>s. The latter will appear to
 * be <code>MembershipEvent</code>s.
 * </p>
 *
 * <p>
 * Similarly, the following code illustrates how a client application would use
 * <code>UniversalMembershipListenerAdapter</code>, where MyMembershipListenerImpl is a
 * subclass.Simply by constructing this subclass of <code>UniversalMembershipListenerAdapter</code>
 * it is registering itself as a <code>ClientMembershipListener</code>:
 *
 * <pre>
 * <code>
 * new MyMembershipListenerImpl();
 * </code>
 * </pre>
 *
 * A client that also connects to the <code>DistributedSystem</code> could register with
 * the<code>ManagementService</code> as shown above.
 * </p>
 *
 * <p>
 * It is recommended that subclasses register with the <code>ManagementService</code> using
 * {@link #registerMembershipListener}, as this will prevent duplicate events for members that are
 * both clients and peer members.If duplicate events are acceptable, you may register subclasses
 * using {@link org.apache.geode.management.ManagementService#addMembershipListener
 * ManagementService#addMembershipListener}.
 * </p>
 *
 *
 * @since GemFire 8.0
 */
public abstract class UniversalMembershipListenerAdapter implements MembershipListener {

  /**
   * Default number of historical events to track in order to avoid duplicate events for members
   * that are both clients and peer members; value is 100.
   */
  public static final int DEFAULT_HISTORY_SIZE = 100;

  private final int historySize;
  private final LinkedList<String> eventHistory; // list of String memberIds
  private final Map<String, Boolean> eventJoined; // key: memberId, value: Boolean


  /** Constructs an instance of UniversalMembershipListenerAdapter. */
  public UniversalMembershipListenerAdapter() {
    this(DEFAULT_HISTORY_SIZE);
  }

  /**
   * Constructs an instance of UniversalMembershipListenerAdapter.
   *
   * @param historySize number of historical events to track in order to avoid duplicate events for
   *        members that are both client and peer members; must a number between 10 and
   *        <code>Integer.MAX_INT</code>
   * @throws IllegalArgumentException if historySize is less than 10
   */
  public UniversalMembershipListenerAdapter(int historySize) {
    if (historySize < 10) {
      throw new IllegalArgumentException(
          String.format("Argument historySize must be between 10 and Integer.MAX_INT: %s .",
              Integer.valueOf(historySize)));
    }
    this.historySize = historySize;
    this.eventHistory = new LinkedList<String>();
    this.eventJoined = new HashMap<String, Boolean>();
    ClientMembership.registerClientMembershipListener(this.clientMembershipListener);
  }

  /**
   * Registers this adapter with the <code>ManagementService</code>. Registering in this way allows
   * the adapter to ensure that callback will not be invoked twice for members that have a client
   * connection and a peer connection. If you register with
   * {@link org.apache.geode.management.ManagementService#addMembershipListener} then duplicate
   * events may occur for members that are both client and peer.
   */
  public void registerMembershipListener(ManagementService service) {
    synchronized (this.eventHistory) {
      service.addMembershipListener(this.membershipListener);
    }
  }

  /**
   * Unregisters this adapter with the <code>ManagementService</code>. If registration is performed
   * with {@link #registerMembershipListener} then this method must be used to successfully
   * unregister the adapter.
   */
  public void unregisterMembershipListener(ManagementService service) {
    synchronized (this.eventHistory) {
      service.removeMembershipListener(this.membershipListener);
    }
    unregisterClientMembershipListener();
  }

  /**
   * Registers this adapter as a <code>ClientMembershipListener</code>. Registration is automatic
   * when constructing this adapter, so this call is not necessary unless it was previously
   * unregistered by calling {@link #unregisterClientMembershipListener}.
   */
  public void registerClientMembershipListener() {
    ClientMembership.registerClientMembershipListener(this.clientMembershipListener);
  }

  /**
   * Unregisters this adapter as a <code>ClientMembershipListener</code>.
   *
   * @see #registerClientMembershipListener
   */
  public void unregisterClientMembershipListener() {
    ClientMembership.unregisterClientMembershipListener(this.clientMembershipListener);
  }

  /**
   * Invoked when a member has joined the distributed system. Also invoked when a client has
   * connected to this process or when this process has connected to a <code>CacheServer</code>.
   */
  public void memberJoined(MembershipEvent event) {}

  /**
   * Invoked when a member has gracefully left the distributed system. Also invoked when a client
   * has gracefully disconnected from this process. or when this process has gracefully disconnected
   * from a <code>CacheServer</code>.
   */
  public void memberLeft(MembershipEvent event) {}

  /**
   * Invoked when a member has unexpectedly left the distributed system. Also invoked when a client
   * has unexpectedly disconnected from this process or when this process has unexpectedly
   * disconnected from a <code>CacheServer</code>.
   */
  public void memberCrashed(MembershipEvent event) {}

  /** Adapts ClientMembershipEvent to look like a MembershipEvent */
  public static class AdaptedMembershipEvent implements MembershipEvent {
    private final ClientMembershipEvent event;

    protected AdaptedMembershipEvent(ClientMembershipEvent event) {
      this.event = event;
    }

    /**
     * Returns true if the member is a client to a CacheServer hosted by this process. Returns false
     * if the member is a CacheServer that this process is connected to.
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
      if (other == this)
        return true;
      if (other == null)
        return false;
      if (!(other instanceof AdaptedMembershipEvent))
        return false;
      final AdaptedMembershipEvent that = (AdaptedMembershipEvent) other;

      if (this.event != that.event && !(this.event != null && this.event.equals(that.event)))
        return false;

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

  private final ClientMembershipListener clientMembershipListener = new ClientMembershipListener() {
    public void memberJoined(ClientMembershipEvent event) {
      membershipListener.memberJoined(new AdaptedMembershipEvent(event));
    }

    public void memberLeft(ClientMembershipEvent event) {
      membershipListener.memberLeft(new AdaptedMembershipEvent(event));
    }

    public void memberCrashed(ClientMembershipEvent event) {
      membershipListener.memberCrashed(new AdaptedMembershipEvent(event));
    }
  };

  protected final MembershipListener membershipListener = new MembershipListener() {
    public void memberJoined(MembershipEvent event) {
      if (!isDuplicate(event, true)) {
        UniversalMembershipListenerAdapter.this.memberJoined(event);
      }
    }

    public void memberLeft(MembershipEvent event) {
      if (!isDuplicate(event, false)) {
        UniversalMembershipListenerAdapter.this.memberLeft(event);
      }
    }

    public void memberCrashed(MembershipEvent event) {
      if (!isDuplicate(event, false)) {
        UniversalMembershipListenerAdapter.this.memberCrashed(event);
      }
    }

    protected boolean isDuplicate(MembershipEvent event, boolean joined) {
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
          } else {
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
