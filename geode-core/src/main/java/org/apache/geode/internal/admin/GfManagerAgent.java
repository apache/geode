/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.internal.admin;

import org.apache.geode.distributed.internal.DM;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
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
   * @since GemFire 4.0
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
   * @since GemFire 3.5
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
   * @since GemFire 4.0
   */
  public InternalDistributedSystem getDSConnection();

}
