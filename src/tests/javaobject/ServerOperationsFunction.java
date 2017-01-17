/*
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
 */
package javaobject;

import java.util.*;
import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.*;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;

public class ServerOperationsFunction extends FunctionAdapter implements
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
        System.out.println("in ServerOperationsFunction.execute: no cache found ");
      }
      pr = c.getRegion("TestTCR1");
      if (argument.equalsIgnoreCase("addKey")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.put(key, key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (argument.equalsIgnoreCase("invalidate")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.invalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (argument.equalsIgnoreCase("destroy")) {


        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.destroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult( Boolean.TRUE);

      }
      else if (argument.equalsIgnoreCase("update")) {
        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object value = "update_" + key;
          pr.put(key, value);
        }
        context.getResultSender().lastResult(Boolean.TRUE);

      }
      else if (argument.equalsIgnoreCase("get")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          Object existingValue = null;
          existingValue = pr.get(key);
        }
        context.getResultSender().lastResult(Boolean.TRUE);
      }
      else if (argument.equalsIgnoreCase("localinvalidate")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          pr.localInvalidate(key);
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else if (argument.equalsIgnoreCase("localdestroy")) {

        Iterator iterator = argumentList.iterator();
        while (iterator.hasNext()) {
          Object key = iterator.next();
          try {
            pr.localDestroy(key);
          } catch (EntryNotFoundException ignore) { /*ignored*/ }
        }
        context.getResultSender().lastResult( Boolean.TRUE);
      }
      else
        context.getResultSender().lastResult( Boolean.FALSE);
    }  
  }
  public String getId() {
    return "ServerOperationsFunction";
  }
  
  public boolean optimizeForWrite() {
    return true;
  }

  public void init(Properties props) {
  }

}
