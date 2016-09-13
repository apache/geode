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

import java.util.List;

/**
 * This interface is implemented by clients of a {@link
 * CacheCollector} who want to be notified of updated snapshot views.
 */
public interface SnapshotClient {

  /**
   * Called when a snapshot view is ready for use
   *
   * @param snapshot 
   *        a set of {@link CacheSnapshot}s.
   * @param responders
   *        All of the members ({@link GemFireVM}s) that have
   *        responded to the snapshot request. 
   */
  public void updateSnapshot(CacheSnapshot snapshot, List responders);
}
