/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

package com.gemstone.gemfire.cache.query.facets.lang;

class DerivedEmployee extends Employee
{
    public Address fieldAddress; // this is actually package level access
    
    public Address address()
    {
        System.out.println("In DerivedEmployee#address()");
        return super.address();
    }
}
