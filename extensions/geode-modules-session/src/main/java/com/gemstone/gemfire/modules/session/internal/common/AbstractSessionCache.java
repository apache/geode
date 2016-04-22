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

package com.gemstone.gemfire.modules.session.internal.common;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.internal.DeltaSessionStatistics;
import com.gemstone.gemfire.modules.session.internal.filter.util.TypeAwareMap;
import com.gemstone.gemfire.modules.util.RegionConfiguration;

import java.util.Map;
import javax.servlet.http.HttpSession;

public abstract class AbstractSessionCache implements SessionCache {

  /**
   * The sessionRegion is the <code>Region</code> that actually stores and
   * replicates the <code>Session</code>s.
   */
  protected Region<String, HttpSession> sessionRegion;

  /**
   * The operatingRegion is the <code>Region</code> used to do HTTP operations.
   * if local cache is enabled, then this will be the local <code>Region</code>;
   * otherwise, it will be the session <code>Region</code>.
   */
  protected Region<String, HttpSession> operatingRegion;

  protected Map<CacheProperty, Object> properties =
      new TypeAwareMap<CacheProperty, Object>(CacheProperty.class);

  protected DeltaSessionStatistics statistics;

  /**
   * {@inheritDoc}
   */
  @Override
  public void stop() {
    sessionRegion.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Region<String, HttpSession> getOperatingRegion() {
    return this.operatingRegion;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Region<String, HttpSession> getSessionRegion() {
    return this.sessionRegion;
  }

  protected void createStatistics() {
    this.statistics =
        new DeltaSessionStatistics(getCache().getDistributedSystem(),
            (String) properties.get(CacheProperty.STATISTICS_NAME));
  }

  /**
   * Build up a {@code RegionConfiguraton} object from parameters originally
   * passed in as filter initialization parameters.
   *
   * @return a {@code RegionConfiguration} object
   */
  protected RegionConfiguration createRegionConfiguration() {
    RegionConfiguration configuration = new RegionConfiguration();

    configuration.setRegionName(
        (String) properties.get(CacheProperty.REGION_NAME));
    configuration.setRegionAttributesId(
        (String) properties.get(CacheProperty.REGION_ATTRIBUTES_ID));

    configuration.setEnableGatewayDeltaReplication(
        (Boolean) properties.get(
            CacheProperty.ENABLE_GATEWAY_DELTA_REPLICATION));
    configuration.setEnableGatewayReplication(
        (Boolean) properties.get(CacheProperty.ENABLE_GATEWAY_REPLICATION));
    configuration.setEnableDebugListener(
        (Boolean) properties.get(CacheProperty.ENABLE_DEBUG_LISTENER));

    return configuration;
  }
}
