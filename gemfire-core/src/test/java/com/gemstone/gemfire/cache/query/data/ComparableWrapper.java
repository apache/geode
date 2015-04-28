/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
 * @author vikramj
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
