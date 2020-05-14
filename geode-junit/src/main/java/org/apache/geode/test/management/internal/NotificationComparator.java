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
package org.apache.geode.test.management.internal;

import java.io.Serializable;
import java.util.Comparator;

import javax.management.Notification;

import org.apache.commons.lang3.ObjectUtils;

public class NotificationComparator implements Comparator<Notification>, Serializable {

  private final boolean compareSource;
  private final boolean compareType;
  private final boolean compareMessage;
  private final boolean compareUserData;

  public NotificationComparator() {
    this(new ComparisonOptions());
  }

  public NotificationComparator(ComparisonOptions options) {
    compareSource = options.source();
    compareType = options.type();
    compareMessage = options.message();
    compareUserData = options.userData();
  }

  @Override
  public int compare(Notification a, Notification b) {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    int value = 0;
    if (compareSource) {
      value = compareHashCodes(a.getSource(), b.getSource());
    }
    value =
        compareType ? value == 0 ? ObjectUtils.compare(a.getType(), b.getType()) : value : value;
    if (compareMessage) {
      value = value == 0 ? ObjectUtils.compare(a.getMessage(), b.getMessage()) : value;
    }
    if (compareUserData) {
      value = value == 0 ? compareHashCodes(a.getUserData(), b.getUserData()) : value;
    }
    return value;
  }

  private static int compareHashCodes(Object a, Object b) {
    if (a == b) {
      return 0;
    }
    if (a == null) {
      return -1;
    }
    if (b == null) {
      return 1;
    }
    return Integer.compare(a.hashCode(), b.hashCode());
  }
}
