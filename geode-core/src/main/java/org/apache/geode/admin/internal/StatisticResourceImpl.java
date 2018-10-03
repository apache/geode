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
package org.apache.geode.admin.internal;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.admin.AdminException;
import org.apache.geode.admin.Statistic;
import org.apache.geode.admin.SystemMember;
import org.apache.geode.internal.Assert;
import org.apache.geode.internal.admin.Stat;
import org.apache.geode.internal.admin.StatResource;

/**
 * Provides monitoring of a statistic resource.
 *
 * @since GemFire 3.5
 */
public class StatisticResourceImpl implements org.apache.geode.admin.StatisticResource {

  /** The underlying remote StatResource which this object delegates to */
  protected StatResource statResource;
  /** Displayable name of this statistic resource */
  protected String name;
  /** Description of this statistic resource */
  protected String description;
  /** Classification type of this statistic resource */
  protected String type;
  /** GemFire system member which owns this statistic resource */
  protected SystemMember member;
  /** The array of statistics in this resource */
  protected Statistic[] statistics;

  // -------------------------------------------------------------------------
  // Constructor(s)
  // -------------------------------------------------------------------------

  /**
   * Constructs an instance of StatisticResourceImpl.
   *
   * @param statResource the admin StatResource to manage/monitor
   * @param member the SystemMember owning this resource
   * @exception org.apache.geode.admin.AdminException if unable to create this StatisticResource for
   *            administration
   */
  public StatisticResourceImpl(StatResource statResource, SystemMember member)
      throws org.apache.geode.admin.AdminException {
    this.statResource = statResource;
    this.member = member;
    this.name = this.statResource.getName();
    this.description = this.statResource.getDescription();
    this.type = this.statResource.getType();
  }

  // -------------------------------------------------------------------------
  // Attributes accessors and mutators
  // -------------------------------------------------------------------------

  public String getName() {
    return this.name;
  }

  public String getDescription() {
    return this.description;
  }

  public String getType() {
    return this.type;
  }

  public String getOwner() {
    return this.member.toString();
  }

  public Statistic[] getStatistics() {
    if (this.statistics == null) {
      try {
        refresh();
      } catch (AdminException e) {
        this.statistics = new Statistic[0];
      }
    }
    return this.statistics;
  }

  public long getUniqueId() {
    return this.statResource.getResourceUniqueID();
  }

  // -------------------------------------------------------------------------
  // Operations
  // -------------------------------------------------------------------------

  public void refresh() throws org.apache.geode.admin.AdminException {
    Stat[] stats = null;
    if (this.statResource != null) {
      stats = this.statResource.getStats();
    }
    if (stats == null || stats.length < 1) {
      throw new AdminException(
          String.format("Failed to refresh statistics %s for %s",
              getType() + "-" + getName(), getOwner()));
    }

    if (this.statistics == null || this.statistics.length < 1) {
      // define new statistics instances...
      List statList = new ArrayList();
      for (int i = 0; i < stats.length; i++) {
        statList.add(createStatistic(stats[i]));
      }
      this.statistics = (Statistic[]) statList.toArray(new Statistic[0]);
    } else {
      // update the existing instances...
      for (int i = 0; i < stats.length; i++) {
        updateStatistic(stats[i]);
      }
    }
  }

  // -------------------------------------------------------------------------
  // Non-public implementation methods
  // -------------------------------------------------------------------------

  /**
   * Updates the value of the {@link Statistic} corresponding to the internal
   * {@link org.apache.geode.internal.admin.Stat}
   *
   * @param stat the internal stat to use in updating the matching statistic
   */
  private void updateStatistic(Stat stat) {
    for (int i = 0; i < this.statistics.length; i++) {
      if (this.statistics[i].getName().equals(stat.getName())) {
        ((StatisticImpl) this.statistics[i]).setStat(stat);
        return;
      }
    }
    Assert.assertTrue(false, "Unknown stat: " + stat.getName());
  }

  /**
   * Creates a new {@link StatisticImpl} to represent the internal
   * {@link org.apache.geode.internal.admin.Stat}
   *
   * @param stat the internal stat to wrap in a new statistic
   */
  protected Statistic createStatistic(Stat stat) {
    return new StatisticImpl(stat);
  }

  /**
   * Returns a string representation of the object.
   *
   * @return a string representation of the object
   */
  @Override
  public String toString() {
    return getName();
  }

}
