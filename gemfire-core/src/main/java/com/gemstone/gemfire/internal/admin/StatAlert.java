/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin;

import java.io.Serializable;
import java.util.Date;

/**
 * This class defines the Alert sent by the AlertManager to the Alert
 * Aggregator. This class includes the information about the
 * <code>AlertDefinition<code> for which this alert being raised 
 * & the value of the statistic. 
 * 
 * @author Hrishi
 */

public class StatAlert implements Serializable {
  private static final long serialVersionUID = 5725457607122449170L;

  private int definitionId;

  private Number[] values;

  private Date time;

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("StatAlert[");
    sb.append("def=" + definitionId);
    sb.append("; values (" + values.length + ") = [");
    for (int i = 0; i < values.length; i ++) {
      sb.append(values[i].toString());
      if (i != values.length - 1) {
        sb.append(", ");
      }
    } // for
    sb.append("]; time = " + time.toString());
    sb.append("]");
    return sb.toString();
  }
  /**
   * The default constructor.
   */
  public StatAlert() {

    definitionId = -1;
    values = null;
    time = null;
  }

  /**
   * The non default constructor.
   * 
   * @param definitionId
   *                StatAlertDefinition identifer value
   * @param values
   *                actual value of the statistic.
   */
  public StatAlert(int definitionId, Number[] values) {
    this.definitionId = definitionId;
    this.values = values;
  }

  /**
   * This function returns the definition identifier for this StatAlert.
   * 
   * @return StatAlertDefinition identifier.
   */
  public int getDefinitionId() {
    return definitionId;
  }

  /**
   * This function sets the definition identifier for this alert.
   * 
   * @param definitionId
   *                StatAlertDefinition identifier.
   */
  public void setDefinitionId(int definitionId) {
    this.definitionId = definitionId;
  }

  /**
   * This method returns the statistic value for this alert. This can either be
   * the latest value for the statistic or the delta between the last two
   * readings, depending on the alert definition.
   * 
   * @return the value.
   */
  public Number[] getValues() {
    return values;
  }

  /**
   * This method sets the value of the statistic for this alert. This can either
   * be the latest value for the statistic or the delta between the last two
   * readings, depending on the alert definition.
   * 
   * @param values
   *                value to be set.
   */
  public void setValues(Number[] values) {
    this.values = values;
  }

  /**
   * This method returns the timestamp when this alert was created.
   * 
   * @return timestamp when this alert was created.
   */
  public Date getTime() {
    return time;
  }

  /**
   * This method sets the timestamp when this alert was created.
   * 
   * @param time
   *                timestamp when this alert was created.
   */
  public void setTime(Date time) {
    this.time = time;
  }

}
