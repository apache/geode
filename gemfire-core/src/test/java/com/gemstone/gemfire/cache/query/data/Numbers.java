/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Numbers.java
 *
 * Created on November 9, 2005, 12:13 PM
 */

package com.gemstone.gemfire.cache.query.data;

/**
 *
 * @author  prafulla
 */
import java.io.Serializable;

public class Numbers implements Serializable{
    ///////fields of class
    public int id;
    public int id1;
    public int id2;
    public float avg1;
    public float max1;
    public double range;
    public long l;
    
    /** Creates a new instance of Numbers */
    public Numbers(int i) {
        id = i;
        id1 = -1*id;
        id2 = 1000 - id;
        avg1 = (id + id1 + id2) / 3;
        max1 = id;
        range = (id - id1);
        l = id*100000000;
    }
    
}
