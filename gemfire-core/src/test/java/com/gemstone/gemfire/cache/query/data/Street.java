/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Street.java
 *
 * Created on September 30, 2005, 1:26 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author vjadhav
 */
public class Street {
    public String street;
    public String lane;
    /** Creates a new instance of Street */
    public Street(String street,String lane) {
        this.street = street;
        this.lane = lane;
    }
    
}
