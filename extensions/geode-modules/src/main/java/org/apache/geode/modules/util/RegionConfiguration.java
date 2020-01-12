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
package org.apache.geode.modules.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CustomExpiry;

/**
 * Class <code>RegionConfiguration</code> encapsulates the configuration attributes for a
 * <code>Region</code> to be created on the server.
 *
 * @since GemFire 6.5
 */
@SuppressWarnings("serial")
public class RegionConfiguration implements DataSerializable {

  /**
   * The name of the <code>Region</code> to be created
   */
  private String regionName;

  /**
   * The id of the <code>RegionAttributes</code> to be used
   */
  private String regionAttributesId;

  /**
   * The default max inactive interval. The default value is -1.
   */
  public static final int DEFAULT_MAX_INACTIVE_INTERVAL = -1;

  /**
   * The maximum time interval in seconds before entries are expired
   */
  private int maxInactiveInterval = DEFAULT_MAX_INACTIVE_INTERVAL;

  /**
   * The <code>CustomExpiry</code> to be used
   */
  private CustomExpiry customExpiry;

  /**
   * Whether delta replication across a <code>Gateway</code> is enabled.
   */
  private boolean enableGatewayDeltaReplication = false;

  /**
   * Whether replication across a <code>Gateway</code> is enabled.
   */
  private boolean enableGatewayReplication = false;

  /**
   * Whether to add a <code>DebugCacheListener</code> to the <code>Region</code>.
   */
  private boolean enableDebugListener = false;

  /**
   * Whether to add a cache listener for session expiration events
   */
  private boolean enableSessionExpirationCacheListener = false;

  /**
   * name for the CacheWriter to be associated with this region. This cache writer must have a zero
   * arg constructor and must be present on the classpath on the server.
   */
  private String cacheWriterName;

  /**
   * Default constructor used by the <code>DataSerialiable</code> interface
   */
  public RegionConfiguration() {}

  /**
   * Sets the name of the <code>Region</code> to be created
   *
   * @param regionName The name of the <code>Region</code> to be created
   */
  public void setRegionName(String regionName) {
    this.regionName = regionName;
  }

  /**
   * Returns the name of the <code>Region</code> to be created
   *
   * @return the name of the <code>Region</code> to be created
   */
  public String getRegionName() {
    return this.regionName;
  }

  /**
   * Sets the id of the <code>RegionAttributes</code> to be used
   *
   * @param regionAttributesId The id of the <code>RegionAttributes</code> to be used
   */
  public void setRegionAttributesId(String regionAttributesId) {
    this.regionAttributesId = regionAttributesId;
  }

  /**
   * Returns the id of the <code>RegionAttributes</code> to be used
   *
   * @return the id of the <code>RegionAttributes</code> to be used
   */
  String getRegionAttributesId() {
    return this.regionAttributesId;
  }

  /**
   * Sets the maximum time interval in seconds before entries are expired
   *
   * @param maxInactiveInterval The maximum time interval in seconds before entries are expired
   */
  public void setMaxInactiveInterval(int maxInactiveInterval) {
    this.maxInactiveInterval = maxInactiveInterval;
  }

  /**
   * Returns the maximum time interval in seconds entries are expired
   *
   * @return the maximum time interval in seconds before entries are expired
   */
  public int getMaxInactiveInterval() {
    return this.maxInactiveInterval;
  }

  /**
   * Sets the <code>CustomExpiry</code> to be used
   *
   * @param customExpiry The <code>CustomExpiry</code> to be used
   */
  public void setCustomExpiry(CustomExpiry customExpiry) {
    this.customExpiry = customExpiry;
  }

  /**
   * Returns the <code>CustomExpiry</code> to be used
   *
   * @return the <code>CustomExpiry</code> to be used
   */
  CustomExpiry getCustomExpiry() {
    return this.customExpiry;
  }

  /**
   * Enables/disables delta replication across a <code>Gateway</code>.
   *
   * @param enableGatewayDeltaReplication true to enable, false to disable gateway delta
   *        replication.
   */
  public void setEnableGatewayDeltaReplication(boolean enableGatewayDeltaReplication) {
    this.enableGatewayDeltaReplication = enableGatewayDeltaReplication;
  }

  /**
   * Returns whether delta replication across a <code>Gateway</code> is enabled.
   *
   * @return whether delta replication across a <code>Gateway</code> is enabled
   */
  boolean getEnableGatewayDeltaReplication() {
    return this.enableGatewayDeltaReplication;
  }

  /**
   * Enables/disables replication across a <code>Gateway</code>.
   *
   * @param enableGatewayReplication true to enable, false to disable gateway replication.
   */
  public void setEnableGatewayReplication(boolean enableGatewayReplication) {
    this.enableGatewayReplication = enableGatewayReplication;
  }

  /**
   * Enables/disables a debug <code>CacheListener</code>.
   *
   * @param enableDebugListener true to enable, false to disable debug <code>CacheListener</code>.
   */
  public void setEnableDebugListener(boolean enableDebugListener) {
    this.enableDebugListener = enableDebugListener;
  }

  /**
   * Returns whether a debug <code>CacheListener</code> is enabled.
   *
   * @return whether a debug <code>CacheListener</code> is enabled
   */
  boolean getEnableDebugListener() {
    return this.enableDebugListener;
  }

  public void setSessionExpirationCacheListener(boolean enableSessionExpirationCacheListener) {
    this.enableSessionExpirationCacheListener = enableSessionExpirationCacheListener;
  }

  boolean getSessionExpirationCacheListener() {
    return this.enableSessionExpirationCacheListener;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeString(this.regionAttributesId, out);
    DataSerializer.writePrimitiveInt(this.maxInactiveInterval, out);
    DataSerializer.writeObject(this.customExpiry, out);
    DataSerializer.writeBoolean(this.enableGatewayDeltaReplication, out);
    DataSerializer.writeBoolean(this.enableGatewayReplication, out);
    DataSerializer.writeBoolean(this.enableDebugListener, out);
    DataSerializer.writeString(this.cacheWriterName, out);
    DataSerializer.writeBoolean(this.enableSessionExpirationCacheListener, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this.regionName = DataSerializer.readString(in);
    this.regionAttributesId = DataSerializer.readString(in);
    this.maxInactiveInterval = DataSerializer.readPrimitiveInt(in);
    this.customExpiry = DataSerializer.readObject(in);
    this.enableGatewayDeltaReplication = DataSerializer.readBoolean(in);
    this.enableGatewayReplication = DataSerializer.readBoolean(in);
    this.enableDebugListener = DataSerializer.readBoolean(in);
    this.cacheWriterName = DataSerializer.readString(in);

    // This allows for backwards compatibility with 2.1 clients
    if (((InputStream) in).available() > 0) {
      this.enableSessionExpirationCacheListener = DataSerializer.readBoolean(in);
    } else {
      this.enableSessionExpirationCacheListener = false;
    }
  }

  public String toString() {
    return "RegionConfiguration[" + "regionName="
        + this.regionName + "; regionAttributesId=" + this.regionAttributesId
        + "; maxInactiveInterval=" + this.maxInactiveInterval
        + "; enableGatewayDeltaReplication=" + this.enableGatewayDeltaReplication
        + "; enableGatewayReplication=" + this.enableGatewayReplication
        + "; enableDebugListener=" + this.enableDebugListener
        + "; enableSessionExpirationCacheListener="
        + this.enableSessionExpirationCacheListener + "; cacheWriter="
        + this.cacheWriterName + "]";
  }

  String getCacheWriterName() {
    return cacheWriterName;
  }
}
