/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/


package com.gemstone.gemfire.cache.query.facets.lang;


public class Address
{
    public String street;
    private String city;
    public String state;
    public String postalCode;


    public Address()
    {
        if (Employee.rand.nextFloat() < .5)
            street = "20575 NW von Neumann Dr";
        if (Employee.rand.nextFloat() < 0.5)
            city = "Beaverton";
        if (Employee.rand.nextFloat() < 0.5)
            state = "OR";
        if (Employee.rand.nextFloat() < 0.5)
            postalCode = "97006";
    }
    
    
    public String toString()
    {
        return street + '|' + city + ' ' + state + "  " + postalCode;
    }

    public String city()
    {
        return city;
    }

    public void city(String city)
    {
        this.city = city;
    }
    
    
}

    
