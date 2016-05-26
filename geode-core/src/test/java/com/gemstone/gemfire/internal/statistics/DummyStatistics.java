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
package com.gemstone.gemfire.internal.statistics;

import com.gemstone.gemfire.StatisticDescriptor;
import com.gemstone.gemfire.Statistics;
import com.gemstone.gemfire.StatisticsType;

/**
 * @since GemFire 7.0
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
