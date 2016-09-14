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
 *
 * @since GemFire 3.0
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
