/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Address.java
 *
 * Created on March 2, 2005, 12:23 PM
 */

package com.gemstone.gemfire.cache.query.data;

import java.util.*;


/**
 *
 * @author  vikramj
 */
public class Address {
    public String zipCode;
    public String city;
    
    public Set street;
    public Set phoneNo;
    
    /** Creates a new instance of Address */
    public Address(String zipCode,String city) {
        this.zipCode=zipCode;
        this.city=city;
    }
    public Address(String zipCode,String city,Set street,Set phoneNo) {
        this.zipCode=zipCode;
        this.city=city;
        this.street=street;
        this.phoneNo=phoneNo;
    }
    
}//end of class
