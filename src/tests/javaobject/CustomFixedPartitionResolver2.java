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
import java.io.Serializable;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

import org.apache.geode.cache.Declarable;
import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.FixedPartitionResolver;

public class CustomFixedPartitionResolver2 implements FixedPartitionResolver, Declarable{

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
	Integer key = (Integer)opDetails.getKey();	
	Integer newkey = key % 6;
	if ( newkey == 0 )
    {	  
      return "P1";
    }	
	else if ( newkey == 1 ) {	
      return "P2";
    }
	else if ( newkey == 2 ) {	
	  return "P3";
	  }
	else if ( newkey == 3 ) {	
	  return "P4";
	  }
	else if ( newkey == 4 ) {	
	  return "P5";
	  }
	else if ( newkey == 5 ) {	
	  return "P6";
	  }
    else {	
      return "Invalid";
    }
  }

  public String getName() {
    return "CustomFixedPartitionResolver2";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {	
	 Integer key = (Integer)opDetails.getKey();
     return (key + 4) ;
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }

}

