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



public class UG_Student extends Student
{
    private int _year;
    private int _satScore;
    
    public UG_Student()
    {
    }


    public UG_Student(String ssn, String name, Date birthdate,
                     Collection courses, float gpa, Department dept,
                      int year, int satScore)
    {
        super(ssn, name, birthdate, courses, gpa, dept);
        _year = year;
        _satScore = satScore;
    }

    public int getYear()
    {
        return _year;
    }


    public int getSatScore()
    {
        return _satScore;
    }

    public void setYear(int year)
    {
        _year = year;
    }


    public void setSatScore(int satScore)
    {
        _satScore = satScore;
    }
}

    
