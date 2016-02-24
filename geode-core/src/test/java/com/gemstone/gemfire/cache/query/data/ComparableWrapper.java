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

/*
 * ComparableWrapper.java
 *
 * Created on May 11, 2005, 11:43 AM
 */

package com.gemstone.gemfire.cache.query.data;

import java.io.Serializable;

/**
 *
 */
public class ComparableWrapper implements Comparable, Serializable {
    private int val;
    /** Creates a new instance of ComparableWrapper */
    public ComparableWrapper() {
    }
    public ComparableWrapper(int x){
        this.val=x;
    }
    public int getVal() {
        return this.val;
    }
    public int hashCode() {
        return val;
    }
    public int compareTo(Object obj) {
        if(!(obj instanceof ComparableWrapper)){
            throw new ClassCastException("Can't compare Object " + obj + " : Not of type ComparableWrapper");
        } else {
           ComparableWrapper cwObj = (ComparableWrapper) obj;
           if(cwObj.getVal() == this.val) {
             return 0;   
           } else if(cwObj.getVal() > this.val ) {
               return -1;
           } else return 1;
        }
    }
    public boolean equals(Object obj) {
        if(!(obj instanceof ComparableWrapper)){
          return false;
        } else {
           ComparableWrapper cwObj = (ComparableWrapper) obj;
           if(cwObj.getVal() == this.val) {
             return true;   
           } else {
             return false;  
           } 
        }
    }
}
