/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.cache30;

import java.io.IOException;
import java.util.Properties;

import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.internal.cache.xmlcache.Declarable2;
import com.gemstone.gemfire.pdx.PdxReader;
import com.gemstone.gemfire.pdx.PdxSerializer;
import com.gemstone.gemfire.pdx.PdxWriter;

/**
 * @author dsmith
 *
 */
public class TestPdxSerializer implements PdxSerializer, Declarable2 {

  private Properties properties;

  public boolean toData(Object o, PdxWriter out) {
    return false;
  }

  public Object fromData(Class<?> clazz, PdxReader in) {
    return null;
  }

  public void init(Properties props) {
    this.properties = props;
    
  }

  public Properties getConfig() {
    return properties;
  }

  @Override
  public int hashCode() {
    return properties.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if(!(obj instanceof TestPdxSerializer)) {
      return false;
    }
    return properties.equals(((TestPdxSerializer)obj).properties);
  }
  
  
  

}
