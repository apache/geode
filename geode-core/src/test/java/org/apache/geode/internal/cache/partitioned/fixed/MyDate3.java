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
package org.apache.geode.internal.cache.partitioned.fixed;

import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Set;

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;

public class MyDate3 extends Date implements FixedPartitionResolver {

  public MyDate3(long time) {
    super(time);
  }

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
    Date date = (Date) opDetails.getCallbackArgument();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    if (month == 0 || month == 1 || month == 2) {
      return "Q1";
    } else if (month == 3 || month == 4 || month == 5) {
      return "Q2";
    } else if (month == 6 || month == 7 || month == 8) {
      return "Q3";
    } else if (month == 9 || month == 10 || month == 11) {
      return "Q4";
    } else {
      return "Invalid Quarter";
    }
  }

  public String getName() {
    return "MyDate3";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {
    Date date = (Date) opDetails.getCallbackArgument();
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    int month = cal.get(Calendar.MONTH);
    return month;
  }

  public void close() {
    // TODO Auto-generated method stub

  }

}
