/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/


package com.gemstone.gemfire.cache.query.facets.lang;

import java.util.*;

public class Person
{
    private String _ssn;
    private String _name;
    private java.sql.Date _birthdate;
    
    public Person()
    {
    }
    
    
    public Person(String ssn, String name, java.util.Date birthdate)
    {
        _ssn = ssn;
        _name = name;
        _birthdate = new java.sql.Date(birthdate.getTime());
    }
    
    public String toString()
    {
        return getName();
    }
    
    public String getName()
    {
        return _name;
    }


    public String getSSN()
    {
        return _ssn;
    }

    public java.sql.Date getBirthdate()
    {
        return _birthdate;
    }
    
    
    public int getAge()
    {
        Calendar now = Calendar.getInstance();
        Calendar bd = Calendar.getInstance();
        bd.setTime(_birthdate);
        
        Calendar bdThisYear = Calendar.getInstance();
        bdThisYear.setTime(_birthdate);
        bdThisYear.set(Calendar.YEAR, now.get(Calendar.YEAR));

        int age = now.get(Calendar.YEAR) - bd.get(Calendar.YEAR);
        
        if (bdThisYear.after(now))
            age--;

        return age;
    }

    public void setBirthdate(Date bd)
    {
        _birthdate = new java.sql.Date(bd.getTime());
    }


    public void setName(String name)
    {
        _name = name;
    }

    public void setSSN(String ssn)
    {
        _ssn = ssn;
    }
}

