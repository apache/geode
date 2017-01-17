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
import java.io.*;
import org.apache.geode.*; // for DataSerializable
import org.apache.geode.cache.Declarable;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionAdapter;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.cache.execute.ResultSender;
import org.apache.geode.cache.execute.RegionFunctionContext;
import org.apache.geode.cache.partition.PartitionRegionHelper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ExampleFunction extends FunctionAdapter implements Declarable {
  public static final String EXAMPLE_FUNCTION9 = "ExampleFunction9";
  public static final String EXAMPLE_FUNCTION_EXCEPTION = "ExampleFunctionException";
  public static final String EXAMPLE_FUNCTION_RESULT_SENDER = "ExampleFunctionResultSender";
  private static final String ID = "id";
  private static final String HAVE_RESULTS = "haveResults";
  private final Properties props;

  // Default constructor for Declarable purposes
  public ExampleFunction() {
    super();
    this.props = new Properties();
  }

  public ExampleFunction(boolean haveResults, String id) {
    this.props = new Properties();
    this.props.setProperty(HAVE_RESULTS, Boolean.toString(haveResults));
    this.props.setProperty(ID, id);
  }
  
  /**
   * Application execution implementation
   * 
   */
  public void execute(FunctionContext context) {
    String id = this.props.getProperty(ID); 

    if (id.equals(EXAMPLE_FUNCTION9)) {
      execute9(context);
    }
    else if (id.equals(EXAMPLE_FUNCTION_EXCEPTION)) {
      executeException(context);
    }
    else if (id.equals(EXAMPLE_FUNCTION_RESULT_SENDER)) {
      executeResultSender(context);
    }
    else {
      context.getResultSender().lastResult(Boolean.FALSE);
    }
  }  
  
  public void execute9(FunctionContext context) {
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  ExampleFunction9.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        rfContext.getResultSender().lastResult(rfContext.getArguments());
      }
      else if (rfContext.getArguments() instanceof String) {
        String key = (String)rfContext.getArguments();
        if(key.equals("TestingTimeOut")){  
          try{
            synchronized (this) {
              this.wait(2000);
            }
          }catch(InterruptedException e){
            rfContext.getDataSet().getCache().getLogger().warning(
                "Got Exception : Thread Interrupted" + e);
          }
        }
        if(context instanceof RegionFunctionContext){
          RegionFunctionContext prContext = (RegionFunctionContext)context;
          if (PartitionRegionHelper.isPartitionedRegion(prContext.getDataSet())) {
            rfContext.getResultSender().lastResult(
                (Serializable)PartitionRegionHelper.getLocalDataForContext(
                    prContext).get(key));
          }
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        ArrayList vals = new ArrayList();
        for (Iterator i = origKeys.iterator(); i.hasNext();) {
          Object val = null;
          if(context instanceof RegionFunctionContext){
            RegionFunctionContext prContext = (RegionFunctionContext)context;
            val = PartitionRegionHelper.getLocalDataForContext(prContext).get(i.next());
          }else{
            val = rfContext.getDataSet().get(i.next());
          }  
          
          if(val != null)
            rfContext.getResultSender().lastResult((Serializable)val);
          
          if (val != null) {
            vals.add(val);
          }
        }
      }
      else if (rfContext.getArguments() instanceof HashMap) {
        HashMap putData = (HashMap)rfContext.getArguments();
        for (Iterator i = putData.entrySet().iterator(); i.hasNext();) {
          Map.Entry me = (Map.Entry)i.next();
          rfContext.getDataSet().put(me.getKey(), me.getValue());
        }
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
      else {
        rfContext.getResultSender().lastResult(Boolean.TRUE);
      }
    }else{
      context.getResultSender().lastResult(Boolean.TRUE);
    }
  }
  
  private Serializable executeException(FunctionContext context) {
    if (context.getArguments() instanceof Boolean) {
      throw new NullPointerException("I have been thrown from ExampleFunction");
    }
    else if (context.getArguments() instanceof String) {
      String key = (String)context.getArguments();
      return key;
    }
    else if (context.getArguments() instanceof Set) {
      Set origKeys = (Set)context.getArguments();
      ArrayList vals = new ArrayList();
      for (Iterator i = origKeys.iterator(); i.hasNext();) {
        Object val = i.next();
        if (val != null) {
          vals.add(val);
        }
      }
      return vals;
    }
    else {
      return Boolean.FALSE;
    }
  }
   
  private Serializable executeResultSender(FunctionContext context) {
    ResultSender resultSender = context.getResultSender();
    if (context instanceof RegionFunctionContext) {
      RegionFunctionContext rfContext = (RegionFunctionContext)context;
      rfContext.getDataSet().getCache().getLogger().info(
          "Executing function :  ExampleFunctionexecuteResultSender.execute " + rfContext);
      if (rfContext.getArguments() instanceof Boolean) {
        if (this.hasResult()) {
          resultSender.lastResult(rfContext.getArguments());
        }
      }
      else if (rfContext.getArguments() instanceof Set) {
        Set origKeys = (Set)rfContext.getArguments();
        Object[] objectArray = origKeys.toArray();
        int size = objectArray.length;
        int i = 0;
        for( ;i < (size-1) ;i++){
          Object val = PartitionRegionHelper.getLocalDataForContext(rfContext).get(objectArray[i]);
          if (val != null) {
            resultSender.sendResult((Serializable)val);
          }
        }
        resultSender.lastResult((Serializable)objectArray[i]);
      }
      else {
        resultSender.lastResult(Boolean.FALSE);
      }
    }else{
      resultSender.lastResult(Boolean.FALSE);
    }
     return null;
  }
  
  /**
   * Get the function identifier, used by clients to invoke this function
   * 
   * @return an object identifying this function
   */
  public String getId() {
    return this.props.getProperty(ID);
  }

  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (!(obj instanceof ExampleFunction)) {
      return false;      
    }
    ExampleFunction function = (ExampleFunction)obj;
    if (!this.props.equals(function.getConfig())) {
      return false;
    }
    return true;
  }

  public boolean hasResult() {
    return Boolean.valueOf(this.props.getProperty(HAVE_RESULTS)).booleanValue();
  }

  public Properties getConfig() {
    return this.props;
  }

  /* (non-Javadoc)
   * @see org.apache.geode.cache.Declarable#init(java.util.Properties)
   */
  public void init(Properties props) {
    this.props.putAll(props);
  }
}
