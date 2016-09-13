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
package org.apache.geode.internal.admin;

import org.apache.geode.DataSerializable;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.admin.statalerts.StatisticInfo;

/**
 * Base interface that supports a StatAlertDefinition
 * 
 */
public interface StatAlertDefinition extends DataSerializable {
  /**
   * Get the unique ID for AlertDefinition, created using the hashCode of
   * StringRepresentation
   */
  public int getId();

  public StatisticInfo[] getStatisticInfo();

  public void setStatisticInfo(StatisticInfo[] info);

  // public boolean isADecoratorOf(Constant decoType [Function,
  // SystemWideFunction, Gauge, Number]);

  /**
   * Evaluate using the passed params and not using the getValue
   */
  public boolean evaluate(Number[] params);

  /**
   * Evaluate the said AlertDefinition. Value of Statistic would be gotten from
   * StatisticInfo Objects
   */
  public boolean evaluate();

  /**
   * Evaluate using the passed params and not using the getValue Return a
   * StatAlert object if one is raised
   */
  public StatAlert evaluateAndAlert(Number[] params);

  /**
   * Evaluate the said AlertDefinition. Value of Statistic would be gotten from
   * StatisticInfo Objects. Return a StatAlert object if one is raised
   */
  public StatAlert evaluateAndAlert();

  /**
   * Get the value of the Statistic this Definition object is created for
   * Depending on the Decorator the value returned can be different
   * 
   * @return Number[]
   */
  public Number[] getValue();

  /**
   * Get the value of the Statistic this Definition object is created for
   * Depending on the Decorator the value returned can be different
   * 
   * @return Number[]
   */
  public Number[] getValue(Number[] vals);

  public boolean verify(StatisticsFactory factory);

  public boolean hasDecorator(String decoratorID);

  public StatAlertDefinition getDecorator(String decoratorID);

  public String getStringRepresentation();

  /**
   * This method returns the name of this stat alert definition.
   * 
   * @return Name of the StatAlertDefinition
   */
  public String getName();

  /**
   * This method sets the name of this stat alert definition.
   * 
   * @param name
   *                name to be set for this StatAlertDefinition.
   */
  public void setName(String name);
}
