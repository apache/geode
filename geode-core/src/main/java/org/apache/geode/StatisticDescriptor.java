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
package org.apache.geode;

//import org.apache.geode.internal.Assert;
//import org.apache.geode.internal.FieldInfo;

/**
 * Describes an individual statistic whose value is updated by an
 * application and may be archived by GemFire.  These descriptions are
 * gathered together in a {@link StatisticsType}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 * <P>
 * <code>StatisticDescriptor</code>s are naturally ordered by their name.
 *
 *
 * @since GemFire 3.0
 */
public interface StatisticDescriptor extends Comparable<StatisticDescriptor> {

  ////////////////////  Instance Methods  ////////////////////

  /**
   * Returns the id of this statistic in a {@link StatisticsType
   * statistics type}.  The id is initialized when its statistics
   * type is created.
   *
   * @throws IllegalStateException
   *         The id has not been initialized yet
   */
  public int getId();

  /**
   * Returns the name of this statistic
   */
  public String getName();

  /**
   * Returns a description of this statistic
   */
  public String getDescription();

  /**
   * Returns the type of this statistic
   */
  public Class<?> getType();

  /**
   * Returns true if this statistic is a counter; false if its a gauge.
   * Counter statistics have values that always increase.
   * Gauge statistics have unconstrained values.
   */
  public boolean isCounter();

  /**
   * Returns true if a larger statistic value indicates better performance.
   */
  public boolean isLargerBetter();
  /**
   * Returns the unit in which this statistic is measured
   */
  public String getUnit();
}
