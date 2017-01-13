/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;

import java.util.*;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.*;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;

public class ServerOperationsWithOutResultFunction extends FunctionAdapter implements
    Declarable {

  public void execute(FunctionContext context) {
    if( context.getArguments() instanceof Vector){
      Region pr = null;
      String argument;
      Vector argumentList;
      argumentList = (Vector)context.getArguments();
      argument = (String)argumentList.remove(argumentList.size() - 1);
      Cache c = null;
      try {
        c = CacheFactory.getAnyInstance();
      } catch (CacheClosedException ex)
      {
        System.out.println("in ServerOperationsWithOutResultFunction.execute: no cache found ");
      }
      pr = c.getRegion("TestTCR1");
      if (argument.equalsIgnoreCase("addKey")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.put(key, key);
        }
      }
      else if (argument.equalsIgnoreCase("invalidate")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
        }
      }
      else if (argument.equalsIgnoreCase("destroy")) {


        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }

      }
      else if (argument.equalsIgnoreCase("update")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object value = "update_" + key;
          pr.put(key, value);
        }

      }
      else if (argument.equalsIgnoreCase("get")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object existingValue = null;
          existingValue = pr.get(key);
        }
      }
      else if (argument.equalsIgnoreCase("localinvalidate")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
        }
      }
      else if (argument.equalsIgnoreCase("localdestroy")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
      }
    } 
  }
  public String getId() {
    //return this.getClass().getName();
    return "ServerOperationsWithOutResultFunction";
  }

  public boolean hasResult() {
    return false;
  }
  
  public boolean isHA() {
  	return false;
  }
  public boolean optimizeForWrite() {
    return true;
  }

  public void init(Properties props) {
  }

}
