/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.modules.session.common;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.modules.session.catalina.internal.DeltaSessionStatistics;
import com.gemstone.gemfire.modules.session.filter.util.TypeAwareMap;
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
