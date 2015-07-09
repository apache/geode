/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/


package com.gemstone.gemfire.cache.query.facets.lang;

import com.gemstone.gemfire.cache.query.CacheUtils;


//import java.util.*;


public class Course
{
    private String _title;
    private String _courseNum;
    private Department _dept;


    public Course()
    {
    }

    public Course(String title, String courseNum, Department department)
    {
        _title = title;
        _courseNum = courseNum;
        _dept = department;
        
    }

    public String toString()
    {
        return getCourseNumber() + ':'+ getTitle();
    }
    

    public String getTitle()
    {
        return _title;
    }

    public String getCourseNumber()
    {
        return _courseNum;
    }

    public Department getDepartment()
    {
        return _dept;
    }

    public String getDeptId()
    {
        CacheUtils.log(this);
        return getCourseNumber().substring(0,3);
    }
    


    public void setTitle(String title)
    {
        _title = title;
    }


    public void setCourseNumber(String courseNum)
    {
        _courseNum = courseNum;
    }

    public void setDepartment(Department dept)
    {
        _dept = dept;
    }

    
}
