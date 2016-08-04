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
package com.gemstone.gemfire.security;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Properties;

import org.apache.geode.security.PostProcessor;

import com.gemstone.gemfire.pdx.SimpleClass;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;

public class PDXPostProcessor implements PostProcessor{
  public static byte[] BYTES = {1,0};

  private boolean pdx = false;
  private int count = 0;

  public void init(Properties props){
    pdx = Boolean.parseBoolean(props.getProperty("security-pdx"));
    count = 0;
  }
  @Override
  public Object processRegionValue(final Serializable principal,
                                   final String regionName,
                                   final Object key,
                                   final Object value) {
    count ++;
    if(value instanceof byte[]){
      assertTrue(Arrays.equals(BYTES, (byte[])value));
    }
    else if(pdx){
      assertTrue(value instanceof PdxInstanceImpl);
    }
    else {
      assertTrue(value instanceof SimpleClass);
    }
    return value;
  }

  public int getCount(){
    return count;
  }
}
