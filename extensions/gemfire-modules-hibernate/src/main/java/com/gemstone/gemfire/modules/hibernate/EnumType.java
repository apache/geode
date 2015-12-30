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
package com.gemstone.gemfire.modules.hibernate;

import java.io.Serializable;

import org.hibernate.HibernateException;

/**
 * Extends {@link org.hibernate.type.EnumType} so as to
 * override methods responsible for cached representation
 * of enums in hibernate.
 * This class must be used in place of {@link org.hibernate.type.EnumType}
 * in client-server topology when the application classes are
 * not available on the server.
 * e.g. a typical enum configuration should look like this:
 * <pre>
 * &lt;property name="myEnum"&gt;
 *   &lt;type name="<b>com.gemstone.gemfire.modules.hibernate.EnumType</b>"&gt;
 *     &lt;param name="enumClass"&gt;com.mycompany.MyEntity$MyEnum&lt;/param&gt;
 *     &lt;param name="type"&gt;12&lt;/param&gt;
 *   &lt;/type&gt;
 * &lt;/property&gt;
 * </pre>
 * @author sbawaska
 */
public class EnumType extends org.hibernate.type.EnumType {

  private static final long serialVersionUID = 3414902482639744676L;
  
  @Override
  public Object assemble(Serializable cached, Object owner)
      throws HibernateException {
    String name = (String) cached;
    Class<? extends Enum> clazz = returnedClass();
    return Enum.valueOf(clazz, name);
  }
  
  @Override
  public Serializable disassemble(Object value) throws HibernateException {
    return ((Enum)value).name();
  }
  
}
