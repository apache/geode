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
package org.apache.geode.internal.admin;

import java.io.Serializable;
import java.util.Date;

/**
 * This class defines the Alert sent by the AlertManager to the Alert Aggregator. This class
 * includes the information about the <code>AlertDefinition<code> for which this alert being raised
 * & the value of the statistic.
 *
 */

public class StatAlert implements Serializable {
  private static final long serialVersionUID = 5725457607122449170L;

  private int definitionId;

  private Number[] values;

  private Date time;

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("StatAlert[");
    sb.append("def=" + definitionId);
    sb.append("; values (" + values.length + ") = [");
    for (int i = 0; i < values.length; i++) {
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
   * @param definitionId StatAlertDefinition identifer value
   * @param values actual value of the statistic.
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
   * @param definitionId StatAlertDefinition identifier.
   */
  public void setDefinitionId(int definitionId) {
    this.definitionId = definitionId;
  }

  /**
   * This method returns the statistic value for this alert. This can either be the latest value for
   * the statistic or the delta between the last two readings, depending on the alert definition.
   *
   * @return the value.
   */
  public Number[] getValues() {
    return values;
  }

  /**
   * This method sets the value of the statistic for this alert. This can either be the latest value
   * for the statistic or the delta between the last two readings, depending on the alert
   * definition.
   *
   * @param values value to be set.
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
   * @param time timestamp when this alert was created.
   */
  public void setTime(Date time) {
    this.time = time;
  }

}
