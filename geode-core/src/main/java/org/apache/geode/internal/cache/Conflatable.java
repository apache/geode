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

package org.apache.geode.internal.cache;

import java.io.Serializable;

/**
 * Interface <code>Conflatable</code> is used by the cache server client notification mechanism to
 * conflate messages being sent from the server to the client.
 *
 *
 * @since GemFire 4.2
 */
public interface Conflatable extends Serializable {

  /**
   * Returns whether the object should be conflated
   *
   * @return whether the object should be conflated
   */
  boolean shouldBeConflated();

  /**
   * Returns the name of the region for this <code>Conflatable</code>
   *
   * @return the name of the region for this <code>Conflatable</code>
   */
  String getRegionToConflate();

  /**
   * Returns the key for this <code>Conflatable</code>
   *
   * @return the key for this <code>Conflatable</code>
   */
  Object getKeyToConflate();

  /**
   * Returns the value for this <code>Conflatable</code>
   *
   * @return the value for this <code>Conflatable</code>
   */
  Object getValueToConflate();

  /**
   * Sets the latest value for this <code>Conflatable</code>
   *
   * @param value The latest value
   */
  void setLatestValue(Object value);

  /**
   * Return this event's identifier
   *
   * @return EventID object uniquely identifying the Event
   */
  EventID getEventId();
}
