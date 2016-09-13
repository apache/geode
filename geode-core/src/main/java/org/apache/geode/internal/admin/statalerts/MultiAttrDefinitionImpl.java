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
package org.apache.geode.internal.admin.statalerts;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;

import org.apache.geode.internal.admin.StatAlert;
import org.apache.geode.internal.admin.StatAlertDefinition;

/**
 * Implementation of {@link StatAlertDefinition} This provides the definition
 * for multiple statistic
 * 
 */
public final class MultiAttrDefinitionImpl implements StatAlertDefinition {
  private static final long serialVersionUID = 2508805676076940969L;

  protected String _name;

  protected int _id;

  protected StatisticInfo[] statisticInfo;

  public MultiAttrDefinitionImpl() {
  }

  /**
   * @param statInfo
   */
  public MultiAttrDefinitionImpl(String name, StatisticInfo[] statInfo) {
    super();
    setStatisticInfo(statInfo);
    this._name = name;
    _id = getName().toUpperCase().hashCode();
  }

  public int getId() {
    return _id;
  }

  @Override // GemStoneAddition
  public int hashCode() {
    return getId();
  }

  public boolean verify(StatisticsFactory factory) {
    if (statisticInfo == null || statisticInfo.length == 0) {
//      System.out.println("No attributes defined for this definition.");
      return false;
    }
    boolean result = false;

    for (int i = 0; i < statisticInfo.length; i++) {

      if (statisticInfo[i] != null) {
        Statistics[] temp = factory.findStatisticsByTextId(statisticInfo[i]
            .getStatisticsTextId());

        if (temp == null || temp.length == 0)
          return false;

        StatisticDescriptor[] temp1 = temp[0].getType().getStatistics();
        for (int j = 0; j < temp1.length; j++) {
          if (statisticInfo[i].getStatisticName().equals(temp1[j].getName())) {
            result = true;
            break;
          }
        }
      }
      else {
        result = false;
        break;
      }
    }

    return result;
  }

  @Override // GemStoneAddition
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("Name:" + getName() + "\n");
    buffer.append("Attributes:\n");
    if (statisticInfo != null) {
      for (int i = 0; i < statisticInfo.length; i++) {
        buffer.append(statisticInfo[i].toString() + "\n");
      }
    }

    return buffer.toString();
  }

  public String getStringRepresentation() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("StatAlertDefinition [\n");
    buffer.append(toString());
    buffer.append("]");

    return buffer.toString();
  }

  /**
   * This method returns the name of this stat alert definition.
   * 
   * @return Name of the StatAlertDefinition
   */
  public String getName() {
    return _name;
  }

  /**
   * This method sets the name of this stat alert definition.
   * 
   * @param name
   *                name to be set for this StatAlertDefinition.
   */
  public void setName(String name) {
    this._name = name;
  }

  public void setStatisticInfo(StatisticInfo[] info) {
    if (info == null || info.length == 0)
      throw new IllegalArgumentException(
          "setStatisticInfo method requires non-zero length array of StatisticInfo objects.");

    statisticInfo = info;
  }

  public StatisticInfo[] getStatisticInfo() {
    return statisticInfo;
  }

  public Number[] getValue() {
    Number[] vals = new Number[statisticInfo.length];
    for (int i = 0; i < vals.length; i++) {
      vals[i] = statisticInfo[i].getStatistics().get(
          statisticInfo[i].getStatisticDescriptor());
    }
    return vals;
  }

  public Number[] getValue(Number[] vals) {
    return vals;
  }

  public boolean evaluate(Number[] params) {
    return evaluate() && params != null;
  }

  public boolean evaluate() {
    return statisticInfo != null && statisticInfo.length != 0;
  }

  public StatAlert evaluateAndAlert(Number[] params) {
    return evaluate() ? getAlert(params) : null;
  }

  public StatAlert evaluateAndAlert() {
    return evaluate() ? getAlert(getValue()) : null;
  }

  protected StatAlert getAlert(Number[] val) {
    return new StatAlert(this.getId(), val);
  }

  public boolean hasDecorator(String decoratorID) {
    return false;
  }

  public StatAlertDefinition getDecorator(String decoratorID) {
    return null;
  }
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(this._name, out);
    DataSerializer.writePrimitiveInt(this._id, out);
    DataSerializer.writeObjectArray(this.statisticInfo, out);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this._name = DataSerializer.readString(in);
    this._id = DataSerializer.readPrimitiveInt(in);
    this.statisticInfo = (StatisticInfo[])DataSerializer.readObjectArray(in);
  }
}
