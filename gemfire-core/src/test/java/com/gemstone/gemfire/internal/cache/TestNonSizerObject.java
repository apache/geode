/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import java.io.Serializable;

/**
 * Test object which does not implement ObjectSizer, used as Key/Value in put operation.
 * 
 * @author arajpal
 * 
 */
public class TestNonSizerObject implements Serializable {

  private static final long serialVersionUID = 0L;

  private String testString;

  public TestNonSizerObject(String testString) {
    super();
    this.testString = testString;
  }

  public String getTestString() {
    return testString;
  }

  public void setTestString(String testString) {
    this.testString = testString;
  }

  @Override
  public int hashCode() {
    return Integer.parseInt(this.testString);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TestNonSizerObject) {
      TestNonSizerObject other = (TestNonSizerObject)obj;
      if (this.testString == other.testString) {
        return true;
      }
    }

    return false;
  }

}
