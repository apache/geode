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

package org.apache.geode.management.configuration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.geode.management.runtime.RuntimeRegionInfo;

/**
 * this holds the region attributes you can configure using management rest api
 *
 * for regions created using gfsh but listed using management rest api, the attributes not supported
 * by management rest api won't be shown.
 */
public class Region extends GroupableConfiguration<RuntimeRegionInfo> {
  public static final String REGION_CONFIG_ENDPOINT = "/regions";

  private String name;
  private RegionType type;

  private String keyConstraint;
  private String valueConstraint;
  private String diskStoreName;
  private Integer redundantCopies;

  private List<Expiration> expirations;
  private Eviction eviction;


  public Region() {}

  @Override
  public boolean isGlobalRuntime() {
    return true;
  }

  @Override
  public Links getLinks() {
    Links links = new Links(getId(), REGION_CONFIG_ENDPOINT);
    links.addLink("indexes", links.getSelf() + "/indexes");
    return links;
  }

  /**
   * Returns {@link #getName()}.
   */
  @Override
  @JsonIgnore
  public String getId() {
    return getName();
  }

  public String getName() {
    return name;
  }

  public void setName(String value) {
    if (value == null) {
      this.name = null;
      return;
    }

    boolean regionPrefixedWithSlash = value.startsWith("/");
    String[] regionSplit = value.split("/");

    boolean hasSubRegions =
        regionPrefixedWithSlash ? regionSplit.length > 2 : regionSplit.length > 1;
    if (hasSubRegions) {
      throw new IllegalArgumentException("Sub-regions are unsupported");
    }

    this.name = regionPrefixedWithSlash ? regionSplit[1] : value;
  }

  public RegionType getType() {
    return type;
  }

  public void setType(RegionType type) {
    this.type = type;
  }

  /**
   * @return the redundant copies of a region
   */
  public Integer getRedundantCopies() {
    return redundantCopies;
  }

  public void setRedundantCopies(Integer redundantCopies) {
    this.redundantCopies = redundantCopies;
  }

  public String getKeyConstraint() {
    return keyConstraint;
  }

  public void setKeyConstraint(String keyConstraint) {
    this.keyConstraint = keyConstraint;
  }

  public String getValueConstraint() {
    return valueConstraint;
  }

  public void setValueConstraint(String valueConstraint) {
    this.valueConstraint = valueConstraint;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    this.diskStoreName = diskStoreName;
  }

  public List<Expiration> getExpirations() {
    return expirations;
  }

  public void setExpirations(List<Expiration> expirations) {
    if (expirations == null) {
      this.expirations = null;
      return;
    }
    this.expirations = expirations.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  public Eviction getEviction() {
    return eviction;
  }

  public void setEviction(Eviction eviction) {
    this.eviction = eviction;
  }

  public void addExpiry(ExpirationType type, Integer timeout, ExpirationAction action) {
    if (expirations == null) {
      expirations = new ArrayList<>();
    }
    expirations.add(new Expiration(type, timeout, action));
  }

  public enum ExpirationType {
    ENTRY_TIME_TO_LIVE,
    ENTRY_IDLE_TIME,
    /**
     * for those other types that could exist in cache.xml but not supported by management api
     * for example: REGION_IDLE_TIME, REGION_TIME_TO_LIVE, eventually these should be removed
     * from the product as well.
     */
    LEGACY
  }

  public enum ExpirationAction {
    DESTROY,
    INVALIDATE,
    /**
     * for those other actions that could exist in cache.xml but not supported by management api
     * for example: LOCAL_DESTROY, LOCAL_INVALIDATE, eventually these should be removed from the
     * product as well.
     */
    LEGACY
  }


  public static class Expiration implements Serializable {
    private ExpirationType type;
    private Integer timeInSeconds;
    private ExpirationAction action;

    public Expiration() {};

    public Expiration(ExpirationType type, Integer timeInSeconds, ExpirationAction action) {
      setType(type);
      this.timeInSeconds = timeInSeconds;
      this.action = action;
    }

    public ExpirationType getType() {
      return type;
    }

    public void setType(ExpirationType type) {
      this.type = type;
    }

    public Integer getTimeInSeconds() {
      return timeInSeconds;
    }

    /**
     * @param timeInSeconds in seconds
     */
    public void setTimeInSeconds(Integer timeInSeconds) {
      this.timeInSeconds = timeInSeconds;
    }

    public ExpirationAction getAction() {
      return action;
    }

    public void setAction(ExpirationAction action) {
      this.action = action;
    }
  }

  public enum EvictionType {
    ENTRY_COUNT,
    MEMORY_SIZE,
    HEAP_PERCENTAGE
  }

  public enum EvictionAction {
    LOCAL_DESTROY,
    OVERFLOW_TO_DISK
  }

  public static class Eviction implements Serializable {
    private EvictionType type;
    private EvictionAction action;
    private Integer entryCount;
    private Integer memorySizeMb;
    private String objectSizer;

    public Eviction() {};

    public Eviction(EvictionType type, EvictionAction action, Integer entryCount,
        Integer memorySizeMb, String objectSizer) {
      this.type = type;
      this.action = action;
      this.entryCount = entryCount;
      this.memorySizeMb = memorySizeMb;
      this.objectSizer = objectSizer;
    };

    public EvictionType getType() {
      return type;
    }

    public void setType(EvictionType type) {
      this.type = type;
    }

    public EvictionAction getAction() {
      return action;
    }

    public void setAction(EvictionAction action) {
      this.action = action;
    }

    public Integer getEntryCount() {
      return entryCount;
    }

    public void setEntryCount(Integer entryCount) {
      this.entryCount = entryCount;
    }

    public Integer getMemorySizeMb() {
      return memorySizeMb;
    }

    public void setMemorySizeMb(Integer memorySizeMb) {
      this.memorySizeMb = memorySizeMb;
    }

    public String getObjectSizer() {
      return objectSizer;
    }

    public void setObjectSizer(String objectSizer) {
      this.objectSizer = objectSizer;
    }
  }
}
