/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.datasource;

/**
 * This class represents the config property for vendor specific data. This has
 * a name, value and type.
 * 
 * @author rreja
 */
public class ConfigProperty  {

  private String _name; // name of property
  private String _value; // value
  private String _type; // Class of the property. It could be one of

  // java.lang.String or Wrapper classes of Basic data types like int, double,
  // boolean etc.
  public ConfigProperty() {
  }

  public ConfigProperty(String name, String value, String type) {
    _name = name;
    _type = type;
    _value = value;
  }

  public void setName(String name) {
    this._name = name;
  }

  public String getName() {
    return _name;
  }

  public void setType(String type) {
    this._type = type;
  }

  public String getType() {
    return _type;
  }

  public void setValue(String value) {
    this._value = value;
  }

  public String getValue() {
    return _value;
  }
}
