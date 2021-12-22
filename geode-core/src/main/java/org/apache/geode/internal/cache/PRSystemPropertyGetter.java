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
/*
 * Created on Dec 22, 2005
 *
 */
package org.apache.geode.internal.cache;

/**
 *
 * TODO To change the template for this generated type comment go to Window - Preferences - Java -
 * Code Style - Code Templates
 */
public class PRSystemPropertyGetter {
  public static int parseInt(String i, int defaultValue) {
    if (i == null || i.equals("")) {
      return defaultValue;
    }
    try {
      return (Integer.parseInt(i));
    } catch (NumberFormatException nfe) {
      return defaultValue;
    }
  }

  public static long parseLong(String l, long defaultValue) {
    if (l == null || l.equals("")) {
      return defaultValue;
    }
    try {
      return (Long.parseLong(l));
    } catch (NumberFormatException nfe) {
      return defaultValue;
    }
  }

  public static boolean booleanvalueOf(String s, boolean defaultValue) {
    if (s == null) {
      return defaultValue;
    }
    try {
      return (Boolean.valueOf(s));
    } catch (NumberFormatException nfe) {
      return defaultValue;
    }

  }
}
