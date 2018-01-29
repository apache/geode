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

package org.apache.geode.distributed.internal;

/**
 * Used to implement statistics on a throttled queue. The implementation will call these methods at
 * to proper time.
 *
 *
 * @since GemFire 3.5
 */
public interface ThrottledQueueStatHelper extends QueueStatHelper {

  /**
   * Called each time a thread was delayed by the throttle.
   */
  void incThrottleCount();

  /**
   * Called after a throttled operation has completed.
   *
   * @param nanos the amount of time, in nanoseconds, the throttle caused us to wait.
   */
  void throttleTime(long nanos);
}
