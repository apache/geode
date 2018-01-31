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
package org.apache.geode.internal.statistics;

/**
 * Describes the contract a VMStats implementation must implement.
 * <p>
 * I named this VMStatsContract because an implementation named VMStats already exists and I didn't
 * want to rename it because of the svn merge issues.
 *
 * @see VMStatsContractFactory
 */
public interface VMStatsContract {
  /**
   * Called by sampler when it wants the VMStats statistics values to be refetched from the system.
   */
  void refresh();

  /**
   * Called by sampler when it wants the VMStats to go away.
   */
  void close();
}
