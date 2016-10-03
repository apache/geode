/*
 *
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
 *
 */
package org.apache.geode.tools.pulse.tests;

public abstract class JMXBaseBean {

  protected abstract String getKey(String propName);

  protected String getString(String key) {
    return JMXProperties.getInstance().getProperty(getKey(key));
  }

  protected String[] getStringArray(String key) {
    return JMXProperties.getInstance().getProperty(getKey(key), "").split(" ");
  }

  protected boolean getBoolean(String key) {
    return Boolean.parseBoolean(JMXProperties.getInstance().getProperty(
        getKey(key)));
  }

  protected int getInt(String key) {
    return Integer.parseInt(JMXProperties.getInstance()
        .getProperty(getKey(key)));
  }

  protected long getLong(String key) {
    return Long.parseLong(JMXProperties.getInstance().getProperty(getKey(key)));
  }

  protected Long[] getLongArray(String key) {
    String value = JMXProperties.getInstance().getProperty(getKey(key), "");
    String[] values = value.split(",");
    Long[] longValues = new Long[values.length];
    for (int i = 0; i < values.length; i++) {
      longValues[i] = Long.parseLong(values[i]);
    }
    return longValues;
  }

  protected double getDouble(String key) {
    return Double.parseDouble(JMXProperties.getInstance().getProperty(
        getKey(key)));
  }

  protected float getFloat(String key) {
    return Float.parseFloat(JMXProperties.getInstance()
        .getProperty(getKey(key)));
  }

}
