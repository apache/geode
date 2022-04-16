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
package org.apache.geode;


/**
 * Describes an individual statistic whose value is updated by an application and may be archived by
 * GemFire. These descriptions are gathered together in a {@link StatisticsType}.
 *
 * <P>
 * To get an instance of this interface use an instance of {@link StatisticsFactory}.
 * <P>
 * <code>StatisticDescriptor</code>s are naturally ordered by their name.
 *
 *
 * @since GemFire 3.0
 */
public interface StatisticDescriptor extends Comparable<StatisticDescriptor> {

  //////////////////// Instance Methods ////////////////////

  /**
   * Returns the id of this statistic in a {@link StatisticsType statistics type}. The id is
   * initialized when its statistics type is created.
   *
   * @return the id of this statistic
   *
   * @throws IllegalStateException The id has not been initialized yet
   */
  int getId();

  /**
   * Returns the name of this statistic
   *
   * @return the name of this statistic
   */
  String getName();

  /**
   * Returns a description of this statistic
   *
   * @return a description of this statistic
   */
  String getDescription();

  /**
   * Returns the type of this statistic
   *
   * @return the type of this statistic
   */
  Class<?> getType();

  /**
   * Returns true if this statistic is a counter; false if its a gauge. Counter statistics have
   * values that always increase. Gauge statistics have unconstrained values.
   *
   * @return whether this statistic is a counter
   */
  boolean isCounter();

  /**
   * Returns true if a larger statistic value indicates better performance.
   *
   * @return whether a larger statistic value indicates better performance
   */
  boolean isLargerBetter();

  /**
   * Returns the unit in which this statistic is measured
   *
   * @return the unit in which this statistic is measured
   */
  String getUnit();
}
