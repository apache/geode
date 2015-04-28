/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import com.gemstone.gemfire.DataSerializable;
import com.gemstone.gemfire.StatisticsFactory;
import com.gemstone.gemfire.internal.admin.statalerts.StatisticInfo;

/**
 * Base interface that supports a StatAlertDefinition
 * 
 * @author hgadre
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
