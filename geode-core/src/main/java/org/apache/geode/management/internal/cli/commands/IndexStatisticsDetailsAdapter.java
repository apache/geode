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

package org.apache.geode.management.internal.cli.commands;

import org.apache.geode.internal.lang.StringUtils;
import org.apache.geode.management.internal.cli.domain.IndexDetails;

public class IndexStatisticsDetailsAdapter {

  private final IndexDetails.IndexStatisticsDetails indexStatisticsDetails;

  public IndexStatisticsDetailsAdapter(
      final IndexDetails.IndexStatisticsDetails indexStatisticsDetails) {
    this.indexStatisticsDetails = indexStatisticsDetails;
  }

  public IndexDetails.IndexStatisticsDetails getIndexStatisticsDetails() {
    return indexStatisticsDetails;
  }

  public String getNumberOfKeys() {
    return getIndexStatisticsDetails() != null
        ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfKeys()) : "";
  }

  public String getNumberOfUpdates() {
    return getIndexStatisticsDetails() != null
        ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfUpdates()) : "";
  }

  public String getNumberOfValues() {
    return getIndexStatisticsDetails() != null
        ? StringUtils.defaultString(getIndexStatisticsDetails().getNumberOfValues()) : "";
  }

  public String getTotalUpdateTime() {
    return getIndexStatisticsDetails() != null
        ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUpdateTime()) : "";
  }

  public String getTotalUses() {
    return getIndexStatisticsDetails() != null
        ? StringUtils.defaultString(getIndexStatisticsDetails().getTotalUses()) : "";
  }
}
