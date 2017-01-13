/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.Properties;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.pdx.PdxInstance;

public class IterateRegion extends FunctionAdapter implements Declarable{

  @Override
  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public void execute(FunctionContext context) {
    Region r = CacheFactory.getAnyInstance().getRegion("DistRegionAck");
    
    for(Object key:r.keySet()) {
      PdxInstance pi = (PdxInstance)r.get(key);
      for(String field : pi.getFieldNames()) {
        Object val = pi.getField(field);
        CacheFactory.getAnyInstance().getLoggerI18n().fine("Pdx " + "Field: " + field + " val:" + val);
      }
    }
    context.getResultSender().lastResult(true);
  }

  @Override
  public String getId() {
    // TODO Auto-generated method stub
    return "IterateRegion";
  }
  

}
