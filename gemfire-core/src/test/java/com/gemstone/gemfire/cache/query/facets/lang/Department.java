/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/


package com.gemstone.gemfire.cache.query.facets.lang;


//import java.util.*;


public class Department
{
    private String _name;
    private String _id;
    private String _office;
    private Faculty _chairperson;


    public Department()
    {
    }

    public Department(String name, String id, String office, Faculty chairperson)
    {
        _name = name;
        _id = id;
        _office = office;
        _chairperson = chairperson;
    }
    

    public String toString()
    {
        return getId();
    }
    

    public String getName()
    {
        return _name;
    }

    public String getId()
    {
        return _id;
    }


    public String getOffice()
    {
        return _office;
    }


    public Faculty getChairperson()
    {
        return _chairperson;
    }


    public void setName(String name)
    {
        _name = name;
    }


    public void setOffice(String office)
    {
        _office = office;
    }


    public void setChairperson(Faculty chairperson)
    {
        _chairperson = chairperson;
    }

    public void setId(String id)
    {
        _id = id;
    }
    
}
