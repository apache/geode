/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * @author Kirk Lund
 * @since 7.0
 */
public class DummyStatistics implements Statistics {

  @Override
  public void close() {
  }

  @Override
  public int nameToId(String name) {
    return 0;
  }
  
  @Override
  public StatisticDescriptor nameToDescriptor(String name) {
    return null;
  }
  
  @Override
  public long getUniqueId() {
    return 0;
  }

  @Override
  public StatisticsType getType() {
    return null;
  }

  @Override
  public String getTextId() {
    return null;
  }

  @Override
  public long getNumericId() {
    return 0;
  }

  @Override
  public boolean isAtomic() {
    return false;
  }

  @Override
  public boolean isClosed() {
    return false;
  }

  @Override
  public void setInt(int id, int value) {
  }

  @Override
  public void setInt(String name, int value) {
  }

  @Override
  public void setInt(StatisticDescriptor descriptor, int value) {
  }

  @Override
  public void setLong(int id, long value) {
  }

  @Override
  public void setLong(StatisticDescriptor descriptor, long value) {
  }

  @Override
  public void setLong(String name, long value) {
  }

  @Override
  public void setDouble(int id, double value) {
  }

  @Override
  public void setDouble(StatisticDescriptor descriptor, double value) {
  }

  @Override
  public void setDouble(String name, double value) {
  }

  @Override
  public int getInt(int id) {
    return 0;
  }

  @Override
  public int getInt(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public int getInt(String name) {
    return 0;
  }

  @Override
  public long getLong(int id) {
    return 0;
  }

  @Override
  public long getLong(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public long getLong(String name) {
    return 0;
  }

  @Override
  public double getDouble(int id) {
    return 0;
  }

  @Override
  public double getDouble(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public double getDouble(String name) {
    return 0;
  }

  @Override
  public Number get(StatisticDescriptor descriptor) {
    return null;
  }

  @Override
  public Number get(String name) {
    return null;
  }

  @Override
  public long getRawBits(StatisticDescriptor descriptor) {
    return 0;
  }

  @Override
  public long getRawBits(String name) {
    return 0;
  }

  @Override
  public void incInt(int id, int delta) {
  }

  @Override
  public void incInt(StatisticDescriptor descriptor, int delta) {
  }

  @Override
  public void incInt(String name, int delta) {
  }

  @Override
  public void incLong(int id, long delta) {
  }

  @Override
  public void incLong(StatisticDescriptor descriptor, long delta) {
  }

  @Override
  public void incLong(String name, long delta) {
  }

  @Override
  public void incDouble(int id, double delta) {
  }

  @Override
  public void incDouble(StatisticDescriptor descriptor, double delta) {
  }

  @Override
  public void incDouble(String name, double delta) {
  }
}
