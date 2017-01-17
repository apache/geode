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
