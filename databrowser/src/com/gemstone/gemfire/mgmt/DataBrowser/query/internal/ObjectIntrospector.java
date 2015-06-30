/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.beans.BeanInfo;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

public class ObjectIntrospector {
  // field exposing 'name' attribute of an enum
  private static final String ENUM_NAME_FIELD = "name";
  // method of enum to get the value of name attribute, ToString method return user friendly name
  private static final String ENUM_NAME_METHOD = "toString";
  //maintains list of classes already introspected
  private static Set<Class<?>> introspectedClassList = new HashSet<Class<?>>();
  
  public static IntrospectionResult introspectClass(Class objClass) throws IntrospectionException {
    LogUtil.info("Start introspecting class :"+objClass);

    IntrospectionRepository repo = IntrospectionRepository.singleton();
    ObjectTypeResultImpl result = new ObjectTypeResultImpl(objClass);
    //Set<ObjectColumn> columns = new HashSet<ObjectColumn>();
    Set<ObjectColumn> columns = new TreeSet<ObjectColumn>(new ObjectColumnComparator());
    //In case of generic Object class, we do not have specific fields/properties to introspect.
    //also in case of Class class, we do not have application specific
    // fields/properties to introspect.
    //Hence we will return immediately.
    if (objClass.equals(java.lang.Object.class)
        || objClass.equals(java.lang.Class.class)
        ||objClass.isInterface()
        ) {
      return result;
    }
    
    //Step 1 : Use the Java Beans specification to retrieve accessible properties.
    BeanInfo info = null;
    try {
      if(objClass.isEnum())
        info = Introspector.getBeanInfo(objClass, Enum.class);//Don't want to introspect unnecessary properties of enum  
      else
        info = Introspector.getBeanInfo(objClass, Object.class);  
    }
    catch (java.beans.IntrospectionException e) {
      throw new IntrospectionException(e);     
    }
    
    //adding class in introspected list for future reference
    introspectedClassList.add(objClass);
    
    PropertyDescriptor[] desc = info.getPropertyDescriptors();
    for (int i = 0; i < desc.length; i++) {
      String name = desc[i].getName();
      Method readMethod = desc[i].getReadMethod();
      if(readMethod == null)
        continue;
      int modifiers = readMethod.getModifiers();
      boolean accessible = readMethod.isAccessible();
      
      if(Modifier.isPublic(modifiers)){
        if(!accessible) {
          try {
            readMethod.setAccessible(true);
          }
          catch (SecurityException e) {
            LogUtil.warning("Exception while making property"+name+" accessible :"+e.getMessage());
            continue;
          }
        }
        
        Method writeMethod = desc[i].getWriteMethod();
        ObjectPropertyImpl column = new ObjectPropertyImpl(name, readMethod, writeMethod);
        Class<?> fieldType = column.getFieldType();
        if (!introspectedClassList.contains(fieldType)) {
          repo.introspectType(fieldType);
        }
        columns.add(column);
        LogUtil.fine("Added Property :" + name);
      }
    }  
    
    //Make a list of public fields available for this type. For this take care of field 'hiding'.
    //For this, we start with the actual type being introspected, upto the base type (i.e. java.lang.Object.class)
    //adding/replacing the fields as necessary.
    Map<String, Field> _fields = new HashMap<String, Field>();
    Class type = objClass;    
    do {
      //Check and add all the public fields.
      Field[] fields = type.getFields();
      for (int i = 0; i < fields.length; i++) {
        Field field = fields[i];
        String name = field.getName();
        int modifier = field.getModifiers();      
        if(!Modifier.isStatic(modifier) && !Modifier.isTransient(modifier)) {
         if(_fields.containsKey(name)) {
           Field temp = _fields.get(name);
           //If the current entry in the Map is of Supertype then replace it with the subtype.
           if((temp != field) && (temp.getDeclaringClass().isAssignableFrom(field.getDeclaringClass()))) {
            _fields.put(name, field); 
           }
         } else {
           _fields.put(name, field);
         }
        }
      }
      type = type.getSuperclass();
    } while(!(type == null  || type.equals(java.lang.Object.class)));

    
    Field[] fields = _fields.values().toArray(new Field[0]);
    for (int i = 0; i < fields.length; i++) {
      Field field = fields[i];
      LogUtil.fine("Field :"+field);
      int modifier = field.getModifiers();      
      boolean readOnly = Modifier.isFinal(modifier);        
      ObjectFieldImpl column = new ObjectFieldImpl(field, readOnly);
      Class<?> fieldType = column.getFieldType();
      if (!introspectedClassList.contains(fieldType)) {
        repo.introspectType(fieldType);
      }
        
      if(!columns.contains(column)) {
       columns.add(column);
       LogUtil.fine("Added Field :"+column.getFieldName());
      } 
    }
    
    // in case of enum, we need to read "name" attribute
    if(objClass.isEnum()){
      Method readMethod;
      try {
        readMethod = objClass.getMethod(ENUM_NAME_METHOD, (Class<?> [])null);
        boolean accessible = readMethod.isAccessible();
        if(!accessible)
          readMethod.setAccessible(true);
        ObjectPropertyImpl column = new ObjectPropertyImpl(ENUM_NAME_FIELD, readMethod, null);
        if(!columns.contains(column)) {
          columns.add(column);
          LogUtil.fine("Added property :"+column.getFieldName());
         } 
      }
      catch (SecurityException e) {
        LogUtil.fine("Exception while making property toString accessible :"+e.getMessage());
      }
      catch (NoSuchMethodException e) {
        LogUtil.fine("Exception while getting method toString :"+e.getMessage());
      }
    }
    
    result.setObjectColumns(columns);  
    LogUtil.fine("End introspecting class :"+objClass);
    return result;
  }
  
  static class ObjectColumnComparator implements Comparator, Serializable {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    public int compare(Object o1, Object o2) {
      ObjectColumn col1 = (ObjectColumn)o1;
      ObjectColumn col2 = (ObjectColumn)o2;
      
      if(col1.getFieldName().equals(col2.getFieldName()))
       return 0; 
      
      if(col1.getDeclaringClass().equals(col2.getDeclaringClass())) 
       return col1.getFieldName().compareTo(col2.getFieldName()); 
      
      if(col1.getDeclaringClass().isAssignableFrom(col2.getDeclaringClass()))
       return -1;
      
      return 1;
    }
  }
}
