/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.distributed.internal.DM;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
//import java.util.*;

/**
 * This interface is the point of entry into a GemFire distributed
 * system for the admin API.  It provides methods for discovering the
 * nodes of a system, registering listeners for nodes which join and
 * leave the system, and for accessing known peers of the manager
 * agent to be used for failover.
 */
public interface GfManagerAgent {
    
  /**
   * Disconnects this agent from the distributed system.
   * @return true if this call resulted in being disconnected
   */
  public boolean disconnect();
    
  /**
   * Returns whether or not the agent is connected to the distributed
   * system. 
   */
  public boolean isConnected();

  /**
   * Returns whether or not this manager agent has created admin
   * objects for the initial members of the distributed system.
   *
   * @since 4.0
   */
  public boolean isInitialized();

  /**
   * Returns whether or not the agent is listening for messages from
   * the distributed system.
   */
  public boolean isListening();
    
  /**
   * Returns information about the application VMs that are members of
   * the distributed system.
   */
  public ApplicationVM[] listApplications();

  /**
   * Returns a list of the other distributed systems that this agent
   * knows about.  Currently (GemFire 3.0) the agent does not know
   * about any other distributed systems.
   */
  public GfManagerAgent[] listPeers();
    
  /**
   * Registers a <code>JoinLeaveListener</code>. on this agent that is
   * notified when membership in the distributed system changes.
   */
  public void addJoinLeaveListener( JoinLeaveListener observer );

  /**
   * Sets the <code>CacheCollector</code> to which this agent delivers
   * {@link CacheSnapshot}s.
   */
  public void setCacheCollector(CacheCollector collector);
    
  /**
   * Deregisters a <code>JoinLeaveListener</code> from this agent.
   */
  public void removeJoinLeaveListener( JoinLeaveListener observer );

//   public void addSnapshotListener(SnapshotListener listener);

//   public void removeSnapshotListener(SnapshotListener listener);

  /**
   * Returns the distribution manager used by this manager agent.
   *
   * @since 3.5
   */
  public DM getDM();

  /**
   * Sets the alert level for this manager agent.
   */
  public void setAlertLevel(int level);

  /**
   * Returns the distributed system that is administered by this
   * agent.
   *
   * @since 4.0
   */
  public InternalDistributedSystem getDSConnection();

}
