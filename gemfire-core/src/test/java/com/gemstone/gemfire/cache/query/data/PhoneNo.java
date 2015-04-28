/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * PhoneNo.java
 *
 * Created on September 29, 2005, 3:40 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author vjadhav
 */
public class PhoneNo {
    public int phoneNo1;
    public int phoneNo2;
    public int phoneNo3;
    public int mobile;
    /** Creates a new instance of PhoneNo */
    public PhoneNo(int i,int j, int k, int m) {
        this.phoneNo1 = i;
        this.phoneNo2 = j;
        this.phoneNo3 = k;
        this.mobile = m;
    }
    
}//end of class
