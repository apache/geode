/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Keywords.java
 *
 * Created on March 10, 2005, 7:01 PM
 */
package com.gemstone.gemfire.cache.query.data;

/**
 * @author vaibhav
 */
public class Keywords {

  //  "select", "distinct", "from", "where", "true", "false","undefined",
  //          "element", "not", "and", "or"};
  //  }
  public boolean select = true;
  public boolean distinct = true;
  public boolean from = true;
  public boolean where = true;
  public boolean undefined = true;
  public boolean element = true;
  public boolean not = true;
  public boolean and = true;
  public boolean or = true;
  public boolean type = true;

  public boolean SELECT() {
    return true;
  }

  public boolean DISTINCT() {
    return true;
  }

  public boolean FROM() {
    return true;
  }

  public boolean WHERE() {
    return true;
  }

  public boolean UNDEFINED() {
    return true;
  }

  public boolean ELEMENT() {
    return true;
  }

  public boolean TRUE() {
    return true;
  }

  public boolean FALSE() {
    return true;
  }

  public boolean NOT() {
    return true;
  }

  public boolean AND() {
    return true;
  }

  public boolean OR() {
    return true;
  }

  public boolean TYPE() {
    return true;
  }
}
