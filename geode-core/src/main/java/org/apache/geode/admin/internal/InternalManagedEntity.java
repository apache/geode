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
package org.apache.geode.admin.internal;

import org.apache.geode.admin.AdminDistributedSystem;
import org.apache.geode.admin.ManagedEntity;
import org.apache.geode.admin.ManagedEntityConfig;

/**
 * Provides internal-only functionality that is expected of all
 * <code>ManagedEntity<code>s.  This functionality is used by the
 * {@link ManagedEntityController} to manage the entity.
 *
 * @since GemFire 4.0
 */
public interface InternalManagedEntity extends ManagedEntity {

  /** The state of a managed entity is unknown. */
  public static final int UNKNOWN = 10;

  /** A managed entity is stopped */
  public static final int STOPPED = 11;

  /** A managed entity is stopping (being stopped) */
  public static final int STOPPING = 12;

  /** A managed entity is starting */
  public static final int STARTING = 13;

  /** A managed entity is running (is started) */
  public static final int RUNNING = 14;

  //////////////////////  Instance Methods  //////////////////////

  /**
   * Returns the <code>ManagedEntityConfig</code> for this
   * <code>ManagedEntity</code>. 
   */
  public ManagedEntityConfig getEntityConfig();

  /**
   * Returns a brief description (such as "locator") of this managed
   * entity. 
   */
  public String getEntityType();

  /**
   * Returns the (local) command to execute in order to start this
   * managed entity.  The command includes the full path to the
   * executable (include <code>$GEMFIRE/bin</code>) and any
   * command-line arguments.  It does not take the {@linkplain
   * ManagedEntityConfig#getRemoteCommand remote command} into account.
   */
  public String getStartCommand();

  /**
   * Returns the (local) command to execute in order to stop this
   * managed entity.
   */
  public String getStopCommand();

  /**
   * Returns the (local) command to execute in order to determine
   * whether or not this managed entity is runing.
   */
  public String getIsRunningCommand();

  /**
   * Returns a descriptive, one-word, unique id for a newly-created
   * <code>ManagedEntity</code>.   This ensures that we do not have
   * collisions in the ids of entities.
   */
  public String getNewId();

  /**
   * Returns the distributed system to which this managed entity
   * belongs.
   */
  public AdminDistributedSystem getDistributedSystem();

  /**
   * Sets the state of this managed entity and informs threads that
   * are waiting for a state change.  See bug 32455.
   *
   * @return The previous state of this managed entity.
   *
   * @see #RUNNING
   */
  public int setState(int state);

}
