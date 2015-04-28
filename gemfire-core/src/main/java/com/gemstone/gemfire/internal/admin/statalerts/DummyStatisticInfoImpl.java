/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.admin.statalerts;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.internal.admin.StatAlertsManager;

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
 * @author hgadre
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
