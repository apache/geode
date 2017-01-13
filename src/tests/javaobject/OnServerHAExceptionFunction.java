/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.execute.InternalFunctionInvocationTargetException;

public class OnServerHAExceptionFunction extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    ArrayList vals = new ArrayList();
    if (context.getArguments() instanceof Boolean) {
      if (context.isPossibleDuplicate()) {
        context.getResultSender().lastResult((Boolean)context.getArguments());
      }
      else {
        throwException();
      }
    }
    else if (context.getArguments() instanceof String) {
      String key = (String)context.getArguments();
      if (context.isPossibleDuplicate()) {
        context.getResultSender().lastResult(key);
      }
      else {
        throwException();
      }
    }
    else if (context.getArguments() instanceof Vector) {
      Cache c = null;
      try {
        c = CacheFactory.getAnyInstance();
      }
      catch (CacheClosedException ex) {
        vals.add("NoCacheResult");
        context.getResultSender().lastResult(vals);
      }
      Region region = c.getRegion("partition_region");
      Vector keys = (Vector)context.getArguments();
      System.out.println("Context.getArguments " + keys);
      Iterator itr = keys.iterator();
      while (itr.hasNext()) {
        Object k = itr.next();
        vals.add(region.get(k));
        System.out.println("vals " + vals);
      }
    }

    if (context.isPossibleDuplicate()) {
      context.getResultSender().lastResult(vals);
    }
    else {
      throwException();
    }

  }

  private void throwException() {
    throw new InternalFunctionInvocationTargetException(
        new NullPointerException(
            "I have been thrown from OnServerHAExceptionFunction"));
  }

  public String getId() {
    return "OnServerHAExceptionFunction";
  }

  public boolean optimizeForWrite() {
    return false;
  }

  public void init(Properties arg0) {

  }

}
