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

package org.apache.geode.management.runtime;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.JsonSerializable;
import org.apache.geode.management.operation.RebalanceOperation;

/**
 * holds the final results of a {@link RebalanceOperation}
 */
@Experimental
public class RebalanceResult implements JsonSerializable {
  private Map<String, Map<String, Long>> rebalanceSummary = new LinkedHashMap<>();

  public RebalanceResult() {}

  /**
   * @return for each region, a map of stat -> value for the statistics reported by rebalance
   */
  public Map<String, Map<String, Long>> getRebalanceSummary() {
    return rebalanceSummary;
  }

  public void setRebalanceSummary(Map<String, Map<String, Long>> rebalanceSummary) {
    this.rebalanceSummary = rebalanceSummary;
  }
}
