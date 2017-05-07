/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package javaobject;


import java.util.*;
import java.io.*;
import com.gemstone.gemfire.*; // for DataSerializable
import com.gemstone.gemfire.cache.Declarable;

import com.gemstone.gemfire.cache.*;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;

public class SingleStrGetFunction extends FunctionAdapter implements Declarable{

  public void execute(FunctionContext context) {
    ArrayList vals = new ArrayList();
    String val = null;
	if (context.getArguments() instanceof Boolean) {
	context.getResultSender().lastResult((Boolean)context.getArguments());
    }
    else if (context.getArguments() instanceof String) {
        String key = (String)context.getArguments();
	context.getResultSender().lastResult(key);
    }
	else if (context.getArguments() instanceof Integer) {
        Integer key = (Integer)context.getArguments();
	context.getResultSender().lastResult(key);
    }
    else if(context.getArguments() instanceof Vector ) {
       Cache c = null;
       try {
           c = CacheFactory.getAnyInstance();
       } catch (CacheClosedException ex)
       {
           vals.add("NoCacheResult");
	   context.getResultSender().lastResult(vals);
       }

       Region region = c.getRegion("partition_region");
       Vector keys = (Vector)context.getArguments();
       System.out.println("Context.getArguments " + keys);
       Iterator itr = keys.iterator();
       
	   while (itr.hasNext()) {
         Object k = itr.next();
		 val = (String)region.get(k);
		 System.out.println("val " + val);
		 break;
       }
    }
    context.getResultSender().lastResult(val);
  }

  public String getId() {
    return "SingleStrGetFunction";
  }

  public void init(Properties arg0) {

  }

}
