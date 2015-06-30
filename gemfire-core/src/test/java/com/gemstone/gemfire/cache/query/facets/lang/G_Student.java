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


public class G_Student extends Student
{
    private String _thesis;
    private Faculty _advisor;
    
    public G_Student()
    {
    }


    public G_Student(String ssn, String name, Date birthdate,
                     Collection courses, float gpa, Department dept,
                     String thesis, Faculty advisor)
    {
        super(ssn, name, birthdate, courses, gpa, dept);
        _thesis = thesis;
        _advisor = advisor;
    }

    public String getThesis()
    {
        return _thesis;
    }


    public Faculty getAdvisor()
    {
        return _advisor;
    }

    public void setThesis(String thesis)
    {
        _thesis = thesis;
    }


    public void setAdvisor(Faculty advisor)
    {
        _advisor = advisor;
    }
}

    
