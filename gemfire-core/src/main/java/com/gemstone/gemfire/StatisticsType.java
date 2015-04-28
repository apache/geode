/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

//import com.gemstone.gemfire.internal.Assert;
//import java.io.*;
//import java.util.*;

/**
 * Used to describe a logical collection of statistics. These descriptions
 * are used to create an instance of {@link Statistics}.
 *
 * <P>
 * To get an instance of this interface use an instance of
 * {@link StatisticsFactory}.
 *
 * @author Darrel Schneider
 *
 * @since 3.0
 */
public interface StatisticsType {

  /**
   * Returns the name of this statistics type
   */
  public String getName();

  /**
   * Returns a description of this statistics type
   */
  public String getDescription();

  /**
   * Returns descriptions of the statistics that this statistics type
   * gathers together
   */
  public StatisticDescriptor[] getStatistics();

  /**
   * Returns the id of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  public int nameToId(String name);
  /**
   * Returns the descriptor of the statistic with the given name in this
   * statistics instance.
   *
   * @throws IllegalArgumentException
   *         No statistic named <code>name</code> exists in this
   *         statistics instance.
   */
  public StatisticDescriptor nameToDescriptor(String name);

}
