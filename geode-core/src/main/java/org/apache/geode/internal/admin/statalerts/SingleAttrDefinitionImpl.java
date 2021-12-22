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
package org.apache.geode.internal.admin.statalerts;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.internal.admin.StatAlert;
import org.apache.geode.internal.admin.StatAlertDefinition;

/**
 * Implementation of {@link StatAlertDefinition} This provides the definition for single statistic
 *
 */
public class SingleAttrDefinitionImpl implements StatAlertDefinition {
  private static final long serialVersionUID = 3292417185742697896L;

  protected String name;

  protected int _id;

  protected StatisticInfo statisticInfo;

  public SingleAttrDefinitionImpl() {}

  public SingleAttrDefinitionImpl(String name, StatisticInfo statisticInfo) {
    super();
    this.statisticInfo = statisticInfo;
    this.name = name;
    _id = getName().toUpperCase().hashCode();
  }

  @Override
  public int getId() {
    return _id;
  }

  @Override // GemStoneAddition
  public int hashCode() {
    return getId();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MultiAttrDefinitionImpl)) {
      return false;
    }
    MultiAttrDefinitionImpl that = (MultiAttrDefinitionImpl) o;
    return _id == that._id;
  }


  @Override
  public boolean verify(StatisticsFactory factory) {
    boolean result = false;

    if (name == null || name.length() == 0) {
      return false;
    }

    if (statisticInfo != null) {
      Statistics[] temp = factory.findStatisticsByTextId(statisticInfo.getStatisticsTextId());

      if (temp == null || temp.length == 0) {
        return false;
      }

      StatisticDescriptor[] temp1 = temp[0].getType().getStatistics();
      for (int i = 0; i < temp1.length; i++) {
        if (statisticInfo.getStatisticName().equals(temp1[i].getName())) {
          result = true;
          break;
        }
      }
    }
    return result;
  }

  @Override
  public String getStringRepresentation() {

    StringBuilder buffer = new StringBuilder();
    buffer.append("StatAlertDefinition [\n");
    buffer.append(this);
    buffer.append("]");

    return buffer.toString();
  }

  @Override // GemStoneAddition
  public String toString() {

    StringBuilder buffer = new StringBuilder();
    buffer.append("Name:" + getName() + "\n");
    buffer.append("Attribute:\n");
    if (statisticInfo != null) {
      buffer.append(statisticInfo + "\n");
    }

    return buffer.toString();
  }

  /**
   * This method returns the name of this stat alert definition.
   *
   * @return Name of the StatAlertDefinition
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * This method sets the name of this stat alert definition.
   *
   * @param name name to be set for this StatAlertDefinition.
   */
  @Override
  public void setName(String name) {
    this.name = name;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.admin.StatAlertDefinition#getStatisticInfo()
   */
  @Override
  public StatisticInfo[] getStatisticInfo() {
    return new StatisticInfo[] {statisticInfo};
  }

  /*
   * (non-Javadoc)
   *
   * @see
   * org.apache.geode.internal.admin.StatAlertDefinition#setStatisticInfo(org.apache.geode.internal.
   * admin.StatisticInfo[])
   */
  @Override
  public void setStatisticInfo(StatisticInfo[] info) {
    if (info == null || info.length != 1) {
      throw new IllegalArgumentException(
          "setStatisticInfo method requires 1 length array of StatisticInfo objects.");
    }

    statisticInfo = info[0];
  }

  @Override
  public Number[] getValue() {
    Number[] vals = new Number[1];
    vals[0] = statisticInfo.getStatistics().get(statisticInfo.getStatisticDescriptor());
    return vals;
  }

  @Override
  public Number[] getValue(Number[] vals) {
    return vals;
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.geode.internal.admin.StatAlertDefinition#evaluate(java.lang.Number[])
   */
  @Override
  public boolean evaluate(Number[] params) {
    return evaluate() && params != null && params.length == 1;
  }

  @Override
  public boolean evaluate() {
    return statisticInfo != null;
  }

  @Override
  public StatAlert evaluateAndAlert(Number[] params) {
    return evaluate(params) ? getAlert(params[0]) : null;
  }

  @Override
  public StatAlert evaluateAndAlert() {
    return evaluate() ? getAlert(getValue()[0]) : null;
  }

  protected StatAlert getAlert(Number val) {
    Number[] vals = new Number[1];
    vals[0] = val;
    return new StatAlert(getId(), vals);
  }

  @Override
  public boolean hasDecorator(String decoratorID) {
    return false;
  }

  @Override
  public StatAlertDefinition getDecorator(String decoratorID) {
    return null;
  }

  @Override
  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeString(name, out);
    DataSerializer.writePrimitiveInt(_id, out);
    DataSerializer.writeObject(statisticInfo, out);
  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    name = DataSerializer.readString(in);
    _id = DataSerializer.readPrimitiveInt(in);
    statisticInfo = DataSerializer.readObject(in);
  }
}
