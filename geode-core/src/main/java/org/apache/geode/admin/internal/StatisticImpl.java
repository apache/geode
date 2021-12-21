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

import org.apache.geode.internal.admin.Stat;

/**
 * Implementation of a single statistic in a <code>StatisticResource</code>
 *
 * @since GemFire 3.5
 *
 */
public class StatisticImpl implements org.apache.geode.admin.Statistic {

  private static final long serialVersionUID = 3899296873901634399L;

  private Stat internalStat;

  protected StatisticImpl() {}

  protected StatisticImpl(Stat internalStat) {
    this.internalStat = internalStat;
  }

  /**
   * @return the identifying name of this stat
   */
  @Override
  public String getName() {
    return internalStat.getName();
  }

  /**
   * @return the value of this stat as a <code>java.lang.Number</code>
   */
  @Override
  public Number getValue() {
    return internalStat.getValue();
  }

  /**
   * @return a display string for the unit of measurement (if any) this stat represents
   */
  @Override
  public String getUnits() {
    return internalStat.getUnits();
  }

  /**
   * @return true if this stat represents a numeric value which always increases
   */
  @Override
  public boolean isCounter() {
    return internalStat.isCounter();
  }

  /**
   * @return the full description of this stat
   */
  @Override
  public String getDescription() {
    return internalStat.getDescription();
  }

  /**
   * Sets the internal stat which allows us to reuse the wrapper object and handle refreshes along
   * with isWriteable set to false on the attribute.
   */
  protected void setStat(Stat internalStat) {
    this.internalStat = internalStat;
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
