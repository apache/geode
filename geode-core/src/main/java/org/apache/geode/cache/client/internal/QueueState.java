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
package org.apache.geode.cache.client.internal;

import java.util.concurrent.ScheduledExecutorService;
import org.apache.geode.internal.cache.EventID;

public interface QueueState {

  public void processMarker();
  public boolean getProcessedMarker();
  public void incrementInvalidatedStats();
  public boolean verifyIfDuplicate(EventID eventId, boolean addToMap);
  public boolean verifyIfDuplicate(EventID eventId);
  /** test hook
   */
  public java.util.Map getThreadIdToSequenceIdMap();
  public void start(ScheduledExecutorService timer, int interval);
}
