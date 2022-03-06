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

package org.apache.geode.redis.internal.eventing;

import java.util.List;

import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.data.RedisKey;

/**
 * Interface intended to be implemented in order to receive Redis events. Specifically this would be
 * keyspace events or blocking commands. EventListeners are registered with the
 * {@link EventDistributor}.
 */
public interface EventListener {

  /**
   * Receive and process an event. This method should execute very quickly. The return value
   * determines additional process steps for the given event.
   *
   * @param commandType the command triggering the event
   * @param key the key triggering the event
   * @return response determining subsequent processing steps
   */
  EventResponse process(RedisCommandType commandType, RedisKey key);

  /**
   * Return the list of keys this listener is interested in.
   */
  List<RedisKey> keys();

  /**
   * Method to resubmit a command if appropriate. This is only relevant for listeners that process
   * events for blocking commands. Listeners that handle keyspace event notification will not use
   * this.
   */
  default void resubmitCommand() {};

  /**
   * Retrieve the timeout for this listener. The default is no timeout.
   */
  default long getTimeout() {
    return 0;
  }

  /**
   * Set a runnable that is responsible for any cleanup when this listener is removed.
   */
  default void setCleanupTask(Runnable r) {}

  /**
   * Called when this listener is removed
   */
  default void cleanup() {}
}
