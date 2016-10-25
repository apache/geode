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

package org.apache.geode.cache.query.facets.lang;


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
