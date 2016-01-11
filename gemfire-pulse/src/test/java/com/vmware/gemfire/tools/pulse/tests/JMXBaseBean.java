/*=========================================================================
 * Copyright (c) 2012-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.vmware.gemfire.tools.pulse.tests;

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
