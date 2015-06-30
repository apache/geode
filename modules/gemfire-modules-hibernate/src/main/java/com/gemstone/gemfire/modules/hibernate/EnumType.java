/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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
