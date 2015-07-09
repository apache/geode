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



public class Student extends Person
{
    private Set _courses;
    private float _gpa;
    private Department _dept;


    public Student()
    {
    }

    public Student(String ssn, String name, Date birthdate,
                   Collection courses, float gpa, Department dept)
    {
        super(ssn, name, birthdate);
        initCourses();
        _courses.addAll(courses);
        _gpa = gpa;
        _dept = dept;
    }

    public Set getCourses()
    {
        if (_courses.isEmpty())
            return Collections.EMPTY_SET;
        return _courses;
    }

    public float getGPA()
    {
        return _gpa;
    }

    public Department getDepartment()
    {
        return _dept;
    }

    public void addCourse(Course course)
    {
        if (_courses == null)
            initCourses();
        _courses.add(course);
    }

    public void removeCourse(Course course)
    {
        if (_courses == null)
            return;
        _courses.remove(course);
        if (_courses.isEmpty())
            _courses = null;
    }

    public void setGPA(float gpa)
    {
        _gpa = gpa;
    }


    public void setDepartment(Department dept)
    {
        _dept = dept;
    }

    private void initCourses()
    {
        //_courses = Utils.getQueryService().newIndexableSet(Course.class);
      _courses = new HashSet();
    }


    
    
    
}

    

    
