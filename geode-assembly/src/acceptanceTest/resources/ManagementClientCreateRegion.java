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

import javax.net.ssl.SSLContext;

import org.apache.geode.management.api.ClusterManagementResult;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.cluster.client.ClusterManagementServiceBuilder;
import org.apache.geode.management.configuration.Region;
import org.apache.geode.management.configuration.RegionType;

public class ManagementClientCreateRegion {
  public static void main(String[] args) throws Exception {
    String regionName = args[0];
    boolean useSsl = Boolean.parseBoolean(args[1]);
    int httpPort = Integer.parseInt(args[2]);

    ClusterManagementService cms;
    if (useSsl) {
      // The default SSLContext will pull in all necessary javax.net.ssl properties
      cms = new ClusterManagementServiceBuilder()
          .setPort(httpPort)
          .setSslContext(SSLContext.getDefault())
          .build();

    } else {
      cms = new ClusterManagementServiceBuilder()
          .setPort(httpPort)
          .build();
    }

    // create region
    Region config = new Region();
    config.setName(regionName);
    config.setType(RegionType.REPLICATE);

    ClusterManagementResult result = cms.create(config);

    if (!result.isSuccessful()) {
      throw new RuntimeException(
          "Failure creating region: " + result.getStatusMessage());
    }

    ClusterManagementResult listResult = cms.list(new Region());
    if (!listResult.isSuccessful()) {
      throw new RuntimeException("failed " + listResult.getStatusMessage());
    }

    System.out.println("Successfully created region: " + regionName);
  }

}
