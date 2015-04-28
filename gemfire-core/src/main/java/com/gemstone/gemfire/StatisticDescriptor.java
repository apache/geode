/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

//import com.gemstone.gemfire.internal.Assert;
//import com.gemstone.gemfire.internal.FieldInfo;

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
 * @author David Whitlock
 * @author Darrel Schneider
 *
 * @since 3.0
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
