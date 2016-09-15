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

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.internal.admin.StatAlertsManager;

/**
 * Implementation {@link StatisticInfo} This does not contains associated
 * statistics object
 * 
 * This has been created client(E.g GFMon2.0), where we actually don't need
 * statistic object. So, Instance of this will be light weight object So that we
 * can reduce the overhead of data transfer across network and
 * serialization/dserialization
 * 
 * on server side , {@link StatAlertsManager} create instance of
 * {@link StatisticInfoImpl} class with help of this class instance
 * 
 */
public class DummyStatisticInfoImpl implements StatisticInfo {
  private static final long serialVersionUID = -5456779525795868187L;

  protected String statisticsTypeName;

  protected String statisticsTextId;

  protected String statisticName;

  public DummyStatisticInfoImpl() {
  }

  /**
   * @param statisticsTextId
   * @param statisticName
   */
  public DummyStatisticInfoImpl(String statisticsTypeName,
      String statisticsTextId, String statisticName) {
    super();
    this.statisticsTypeName = statisticsTypeName;
    this.statisticsTextId = statisticsTextId;
    this.statisticName = statisticName;
  }

  public String getStatisticName() {
    return statisticName;
  }

  public String getStatisticsTextId() {
    return statisticsTextId;
  }

  public void setStatisticName(String statisticName) {
    this.statisticName = statisticName;
  }

  public void setStatisticsTextId(String statisticsTextId) {
    this.statisticsTextId = statisticsTextId;
  }

  public StatisticDescriptor getStatisticDescriptor() {
    throw new UnsupportedOperationException(
        "DummyStatisticInfoImpl class does not support getStatisticDescriptor method.");
  }

  public Statistics getStatistics() {
    throw new UnsupportedOperationException(
        "DummyStatisticInfoImpl class does not support getStatistics method.");
  }

  public String getStatisticsTypeName() {
    return statisticsTypeName;
  }

  public void setStatisticsTypeName(String statisticsType) {
    this.statisticsTypeName = statisticsType;
  }

  public Number getValue() {
    throw new UnsupportedOperationException(
        "DummyStatisticInfoImpl class does not support getValue method.");
  }

  @Override
  public boolean equals(Object object) {

    if (object == null || !(object instanceof DummyStatisticInfoImpl)) {
      return false;
    }

    DummyStatisticInfoImpl other = (DummyStatisticInfoImpl)object;

    if (this.statisticName.equals(other.getStatisticName())
        && this.statisticsTypeName.equals(other.getStatisticsTypeName())
        && this.statisticsTextId != null
        && this.statisticsTextId.equals(other.getStatisticsTextId())) {
      return true;

    }

    return false;
  }

  public void toData(DataOutput out) throws IOException {
    out.writeUTF(this.statisticsTypeName);
    out.writeUTF(this.statisticsTextId);
    out.writeUTF(this.statisticName);
  }

  public void fromData(DataInput in)
    throws IOException, ClassNotFoundException {
    this.statisticsTypeName = in.readUTF();
    this.statisticsTextId = in.readUTF();
    this.statisticName = in.readUTF();
  }

  @Override
  public int hashCode() {
    return (statisticName + ":" + statisticsTextId).hashCode();
  }

  @Override
  public String toString() {
    return this.statisticsTypeName + " [" + this.statisticName + "]";
  }
}
