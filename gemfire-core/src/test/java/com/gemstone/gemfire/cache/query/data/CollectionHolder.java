/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * CollectionHolder.java
 *
 * Created on May 18, 2005, 3:46 PM
 */

package com.gemstone.gemfire.cache.query.data;

import java.io.*;
import java.util.Arrays;

import com.gemstone.gemfire.*;

/**
 *
 * @author kdeshpan
 */
public class CollectionHolder implements Serializable, DataSerializable {
    
    public String [] arr;
    public static String secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP"};
    /** Creates a new instance of CollectionHolder */
    public CollectionHolder() {
      this.arr = new String [10];
      for(int i = 0; i<5; i++) {
         arr[i]=""+i; 
      }
      for(int i = 5; i<10; i++) {
        arr[i]=secIds[i-5]; 
     }
      
    }
    
    public String [] getArr() {
      return this.arr;  
    }

    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      this.arr = DataSerializer.readStringArray(in);
    }
    
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeStringArray(this.arr, out);
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(arr);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!(obj instanceof CollectionHolder)) {
        return false;
      }
      CollectionHolder other = (CollectionHolder)obj;
      if (!Arrays.equals(arr, other.arr)) {
        return false;
      }
      return true;
    } 
    
}
