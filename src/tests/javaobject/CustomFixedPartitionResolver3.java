/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

public class CustomFixedPartitionResolver3 implements FixedPartitionResolver, Declarable{

  public String getPartitionName(EntryOperation opDetails, Set targetPartitions) {
	Integer key = (Integer)opDetails.getKey();
	Integer newkey = key % 3;
	if ( newkey == 0 )
    {
      return "P1";
    }	
	else if ( newkey == 1 ) 
	{
      return "P2";
    }
	else if ( newkey == 2 ) 
	{
	  return "P3";
	}	
    else 
	{
      return "Invalid";
    }
  }

  public String getName() {
    return "CustomFixedPartitionResolver3";
  }

  public Serializable getRoutingObject(EntryOperation opDetails) {	
	 Integer key = (Integer)opDetails.getKey();
     return (key % 5) ;
  }

  public void close() {
    // TODO Auto-generated method stub
    
  }

  public void init(Properties props) {
    // TODO Auto-generated method stub
    
  }

}

