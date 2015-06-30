/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.company.data;
/**
 * A <code>Declarable</code> <code>ObjectSizer</code> for used for XML testing
 *
 * @author Mitch Thomas
 * @since 5.0
 */
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.util.ObjectSizer;

public class MySizer implements ObjectSizer, Declarable {

  String name;

  public int sizeof( Object o ) {
    return ObjectSizer.DEFAULT.sizeof(o);
  }

  public void init(Properties props) {
      this.name = props.getProperty("name", "defaultName");
  }
}
