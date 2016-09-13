/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    
