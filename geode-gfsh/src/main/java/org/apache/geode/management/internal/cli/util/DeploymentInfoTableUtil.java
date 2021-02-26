/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.util;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.geode.management.internal.cli.domain.DeploymentInfo;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;
import org.apache.geode.management.internal.functions.CliFunctionResult;

public class DeploymentInfoTableUtil {
  @SuppressWarnings("unchecked")
  public static List<DeploymentInfo> getDeploymentInfoFromFunctionResults(
      List<CliFunctionResult> functionResults) {
    List<DeploymentInfo> deployedJarInfo = new LinkedList<>();
    for (CliFunctionResult cliResult : functionResults) {
      if (cliResult.getResultObject() instanceof Map) {

        // This is for backwards compatibility during rolling upgrades. Ths branch can be removed
        // when versions prior to 1.14 are retired.
        Map<String, String> infoMap = (Map<String, String>) cliResult.getResultObject();
        if (infoMap != null) {
          for (Map.Entry<String, String> deploymentInfoEntry : infoMap.entrySet()) {
            deployedJarInfo.add(new DeploymentInfo(cliResult.getMemberIdOrName(), "",
                deploymentInfoEntry.getKey(), deploymentInfoEntry.getValue()));
          }
        }
      } else if (cliResult.getResultObject() instanceof List) {
        deployedJarInfo.addAll((List<DeploymentInfo>) cliResult.getResultObject());
      }
    }
    return deployedJarInfo;
  }

  public static void writeDeploymentInfoToTable(String[] columnHeaders,
      TabularResultModel tabularData,
      List<DeploymentInfo> deployedJarInfo) {
    for (DeploymentInfo deploymentInfo : deployedJarInfo) {
      tabularData.accumulate(columnHeaders[0], deploymentInfo.getMemberName());
      tabularData.accumulate(columnHeaders[1], deploymentInfo.getDeploymentName());
      tabularData.accumulate(columnHeaders[2], deploymentInfo.getFileName());
      tabularData.accumulate(columnHeaders[3], deploymentInfo.getAdditionalDeploymentInfo());
    }
  }
}
