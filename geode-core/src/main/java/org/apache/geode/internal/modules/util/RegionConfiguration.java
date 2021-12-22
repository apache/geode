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
package org.apache.geode.internal.modules.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializable;
import org.apache.geode.DataSerializer;
import org.apache.geode.Instantiator;
import org.apache.geode.cache.CacheWriter;
import org.apache.geode.cache.CustomExpiry;

/**
 * Class <code>RegionConfiguration</code> encapsulates the configuration attributes for a
 * <code>Region</code> to be created on the server.
 *
 *
 * @since GemFire 6.5
 */
@SuppressWarnings({"serial"})
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
    return regionName;
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
  public String getRegionAttributesId() {
    return regionAttributesId;
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
    return maxInactiveInterval;
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
  public CustomExpiry getCustomExpiry() {
    return customExpiry;
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
  public boolean getEnableGatewayDeltaReplication() {
    return enableGatewayDeltaReplication;
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
   * Returns whether replication across a <code>Gateway</code> is enabled.
   *
   * @return whether replication across a <code>Gateway</code> is enabled
   */
  public boolean getEnableGatewayReplication() {
    return enableGatewayReplication;
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
  public boolean getEnableDebugListener() {
    return enableDebugListener;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(regionName, out);
    DataSerializer.writeString(regionAttributesId, out);
    DataSerializer.writePrimitiveInt(maxInactiveInterval, out);
    DataSerializer.writeObject(customExpiry, out);
    DataSerializer.writeBoolean(enableGatewayDeltaReplication, out);
    DataSerializer.writeBoolean(enableGatewayReplication, out);
    DataSerializer.writeBoolean(enableDebugListener, out);
    DataSerializer.writeString(cacheWriterName, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    regionName = DataSerializer.readString(in);
    regionAttributesId = DataSerializer.readString(in);
    maxInactiveInterval = DataSerializer.readPrimitiveInt(in);
    customExpiry = DataSerializer.readObject(in);
    enableGatewayDeltaReplication = DataSerializer.readBoolean(in);
    enableGatewayReplication = DataSerializer.readBoolean(in);
    enableDebugListener = DataSerializer.readBoolean(in);
    cacheWriterName = DataSerializer.readString(in);
  }

  /**
   * Registers an <code>Instantiator</code> for the <code>SessionConfiguration</code> class
   */
  public static void registerInstantiator(int id) {
    Instantiator.register(new Instantiator(RegionConfiguration.class, id) {
      @Override
      public DataSerializable newInstance() {
        return new RegionConfiguration();
      }
    });
  }

  public String toString() {
    return new StringBuilder().append("RegionConfiguration[").append("regionName=")
        .append(regionName).append("; regionAttributesId=").append(regionAttributesId)
        .append("; maxInactiveInterval=").append(maxInactiveInterval)
        .append("; enableGatewayDeltaReplication=").append(enableGatewayDeltaReplication)
        .append("; enableGatewayReplication=").append(enableGatewayReplication)
        .append("; enableDebugListener=").append(enableDebugListener).append("; cacheWriter=")
        .append(cacheWriterName).append("]").toString();
  }

  /**
   * set the fully qualified name of the {@link CacheWriter} to be created on the server. The
   * cacheWriter must have a zero arg constructor, and must be present on the classpath on the
   * server.
   *
   * @param cacheWriterName fully qualified class name of the cacheWriter
   */
  public void setCacheWriterName(String cacheWriterName) {
    this.cacheWriterName = cacheWriterName;
  }

  public String getCacheWriterName() {
    return cacheWriterName;
  }
}
