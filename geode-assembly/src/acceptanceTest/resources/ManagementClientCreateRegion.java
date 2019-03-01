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

import org.apache.geode.cache.configuration.RegionConfig;
import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.client.ClusterManagementServiceProvider;

public class ManagementClientTestCreateRegion {
  public static void main(String[] args) {
    String regionName = args[0];

    ClusterManagementService cms =
        ClusterManagementServiceProvider.getService("http://localhost:7070/geode-management");

    RegionConfig config = new RegionConfig();
    config.setName(regionName);
    config.setType("REPLICATE");

    ClusterManagementResult result = cms.create(config, "cluster");

    if (!result.isSuccessful()) {
      throw new RuntimeException(
          "Failure creating region: " + result.getStatusMessage());
    }

    System.out.println("Successfully created region: " + regionName);
  }
}
