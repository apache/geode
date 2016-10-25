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

package org.apache.geode.cache.query.internal;

import java.lang.reflect.AccessibleObject;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Member;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.query.NameNotFoundException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.types.ObjectType;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.FieldNotFoundInPdxVersion;
import org.apache.geode.pdx.internal.PdxInstanceImpl;
import org.apache.geode.pdx.JSONFormatter;

/**
 * Utility for managing an attribute
 *
 * @version     $Revision: 1.1 $
 */


public class AttributeDescriptor {
  private final String _name;
  /** cache for remembering the correct Member for a class and attribute */
  private static final ConcurrentMap _cache = new ConcurrentHashMap();
  
  
  
  public AttributeDescriptor(String name) {
    _name = name;
  }
  
  
  
  /** Validate whether this attribute <i>can</i> be evaluated for target type */
  public boolean validateReadType(Class targetType) {
    try {
      getReadMember(targetType);
      return true;
    } catch (NameNotFoundException e) {
      return false;
    }
  }
  
  public Object read(Object target)
  throws NameNotFoundException, QueryInvocationTargetException {
    if (target == null  || target == QueryService.UNDEFINED){
      return QueryService.UNDEFINED;
    }
    if (target instanceof PdxInstance){
      return readPdx((PdxInstance) target);
    }
    //for non pdx objects
    return read(target, target.getClass());
  }
  
  // used when the resolution of an attribute must be on a superclass
  // instead of the runtime class
  private Object read(Object target, Class resolutionClass)
  throws NameNotFoundException, QueryInvocationTargetException {
    Support.Assert(target != null);
    Support.Assert(target != QueryService.UNDEFINED);
    Member m;
    if (target.getClass().getName().startsWith("org.apache.geode.internal.cache.Token$")) {
      return QueryService.UNDEFINED;
    } else {
      m = getReadMember(resolutionClass);
    }
    try {
      if (m instanceof Method) {
        try {
          if (target.getClass().getName().startsWith("org.apache.geode.internal.cache.Token$")) {
            return QueryService.UNDEFINED;
          } else {            
            return ((Method)m).invoke(target, (Object[])null);
          }
        } catch (EntryDestroyedException e) {
          //eat the Exception
          return QueryService.UNDEFINED;
        } catch (IllegalAccessException e) {
          throw new NameNotFoundException(LocalizedStrings.AttributeDescriptor_METHOD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR.toLocalizedString(new Object[] {m.getName(), target.getClass().getName()}), e);
        } catch (InvocationTargetException e) {
          // if the target exception is Exception, wrap that,
          // otherwise wrap the InvocationTargetException itself
          Throwable t = e.getTargetException();
          if ((t instanceof EntryDestroyedException)) {
             //eat the exception
             return QueryService.UNDEFINED;
          }
          if (t instanceof Exception)
            throw new QueryInvocationTargetException(t);
          throw new QueryInvocationTargetException(e);
        }
      } else {
        try {
          if (target.getClass().getName().startsWith("org.apache.geode.internal.cache.Token$")) {
            return QueryService.UNDEFINED;
          } else {
            return ((Field)m).get(target);
          }  
        } catch (IllegalAccessException e) {
          throw new NameNotFoundException(LocalizedStrings.AttributeDescriptor_FIELD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR.toLocalizedString(new Object[] {m.getName(), target.getClass().getName()}), e);
        } catch (EntryDestroyedException e) {
          return QueryService.UNDEFINED;
        }
      }
    } catch (EntryDestroyedException e) {
      // eat the exception
      return QueryService.UNDEFINED;
    }
  }
  
  
  /* this method is not yet used. Here to support Update statements */
  // returns either null or UNDEFINED
        /*
    public Object write(Object target, Object newValue)
        throws PathEvaluationException
    {
        if (target == null)
            return QueryService.UNDEFINED;
         
         
        Class targetType = target.getClass();
        Class argType = newValue == null ? null : newValue.getClass();
        Member m = getWriteMember(targetType, argType);
        if (m == null)
            throw new PathEvaluationException(LocalizedStrings.AttributeDescriptor_NO_UPDATE_PATH_MAPPING_FOUND_FOR_0.toLocalizedString(_name));
        try
        {
            if (m instanceof Method)
            {
                try
                {
                    ((Method)m).invoke(target, new Object[] { newValue });
                    return null;
                }
                catch (InvocationTargetException e)
                {
                    throw new PathEvaluationException(e.getTargetException());
                }
            }
            else
            {
                ((Field)m).set(target, newValue);
                return null;
            }
        }
        catch (IllegalAccessException e)
        {
            throw new PathEvaluationException(e));
        }
         
    }
         */
  
  
  Member getReadMember(ObjectType targetType)
  throws NameNotFoundException {
    return getReadMember(targetType.resolveClass());
  }
  
  Member getReadMember(Class targetClass)
  throws NameNotFoundException {
    // mapping: public field (same name), method (getAttribute()),
    // method (attribute())    
    List key = new ArrayList();
    key.add(targetClass);
    key.add(_name);
    
    Member m = (Member)_cache.get(key);
    if (m != null)
      return m;
    
    m = getReadField(targetClass);
    if (m == null)
      m = getReadMethod(targetClass);
    if (m != null)
      _cache.putIfAbsent(key, m);
    else
      throw new NameNotFoundException(LocalizedStrings.AttributeDescriptor_NO_PUBLIC_ATTRIBUTE_NAMED_0_WAS_FOUND_IN_CLASS_1.toLocalizedString(new Object[] {_name, targetClass.getName()}));
    // override security for nonpublic derived classes with public members
    ((AccessibleObject)m).setAccessible(true);
    return m;
  }
  
  
        /* Not yet used, Here to support Update statements
    private Member getWriteMember(Class targetType, Class argType)
    {
            // mapping: public field (same name), method (setAttribute(val)),
            // method attribute(val)
        Member m;
        m = getWriteField(targetType, argType);
        if (m != null)
            return m;
        return getWriteMethod(targetType, argType);
    }
         */
  
  
  
  private Field getReadField(Class targetType) {
    try {
      return targetType.getField(_name);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }
  
        /* not yet used
    private Field getWriteField(Class targetType, Class argType)
    {
        try
        {
            return targetType.getField(_name);
        }
        catch (NoSuchFieldException e)
        {
            return null;
        }
    }
         */
  
  
  
  private Method getReadMethod(Class targetType) {
    Method m;
    String beanMethod = "get" + _name.substring(0,1).toUpperCase() + _name.substring(1);
    m = getReadMethod(targetType, beanMethod);
    if (m != null)
      return m;
    return getReadMethod(targetType, _name);
  }
  
        /* not yet used
    private Method getWriteMethod(Class targetType, Class argType)
    {
        Method m;
        String beanMethod = "set" + _name.substring(0,1).toUpperCase() + _name.substring(1);
        m = getWriteMethod(targetType, argType, beanMethod);
        if (m != null)
            return m;
        return getWriteMethod(targetType, argType, _name);
    }
         */
  
  
  private Method getReadMethod(Class targetType, String methodName) {
    try {
      return targetType.getMethod(methodName, (Class[])null);
    } catch (NoSuchMethodException e) {
      updateClassToMethodsMap(targetType.getCanonicalName(), _name);
      return null;
    }
  }
  
        /* not yet used
    private Method getWriteMethod(Class targetType, Class argType, String methodName)
    {
        try
        {
                // @todo look up maximally specific method based on argType
            return targetType.getMethod(methodName, new Class[] { argType });
        }
        catch (NoSuchMethodException e)
        {
            return null;
        }
    }
         */
  /**
   * reads field value from a PdxInstance
   * @param target
   * @return the value of the field from PdxInstance
   * @throws NameNotFoundException
   * @throws QueryInvocationTargetException
   */
  private Object readPdx(PdxInstance target) throws NameNotFoundException,
      QueryInvocationTargetException {
    if (target instanceof PdxInstanceImpl) {
      PdxInstanceImpl pdxInstance = (PdxInstanceImpl) target;
      // if the field is present in the pdxinstance
      if (pdxInstance.hasField(_name)) { 
        // return PdxString if field is a String otherwise invoke readField
        return pdxInstance.getRawField(_name);
      }
      else {
        // field not found in the pdx instance, look for the field in any of the
        // PdxTypes (versions of the pdxinstance) in the type registry
        String className = pdxInstance.getClassName();
        
        //don't look further for field or method or reflect on GemFire JSON data
        if(className.equals(JSONFormatter.JSON_CLASSNAME)){
            return QueryService.UNDEFINED;
        }

        
        // check if the field was not found previously
        if (!isFieldAlreadySearchedAndNotFound(className, _name)) {
          try {
            return pdxInstance.getDefaultValueIfFieldExistsInAnyPdxVersions(
                _name, className);
          } catch (FieldNotFoundInPdxVersion e1) {
            // remember the field that is not present in any version to avoid
            // trips to the registry next time
            updateClassToFieldsMap(className, _name);
          }
        }  
        // if the field is not present in any of the versions try to
        // invoke implicit method call
        if(!this.isMethodAlreadySearchedAndNotFound(className, _name)){
        	try{
        		return readFieldFromDeserializedObject(pdxInstance, target);
        	}catch (NameNotFoundException ex){
				updateClassToMethodsMap(pdxInstance.getClassName(),_name);
				throw ex;
        	}
        }else return QueryService.UNDEFINED;
      }
    }
    else {
      // target could be another implementation of PdxInstance like
      // PdxInstanceEnum, in this case getRawField and getCachedOjects methods are
      // not available
      if (((PdxInstance) target).hasField(_name)) {
        return ((PdxInstance) target).getField(_name);
      }
      throw new NameNotFoundException(
          LocalizedStrings.AttributeDescriptor_FIELD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR
              .toLocalizedString(new Object[] { _name,
                  target.getClass().getName() }));
    }
  }
  
  private Object readFieldFromDeserializedObject(PdxInstanceImpl pdxInstance,
      Object target) throws NameNotFoundException,
      QueryInvocationTargetException {
    try {
      Object obj = pdxInstance.getCachedObject();
      return read(obj, obj.getClass());
    } catch (PdxSerializationException e) {
      throw new NameNotFoundException( // the domain object is not available
          LocalizedStrings.AttributeDescriptor_FIELD_0_IN_CLASS_1_IS_NOT_ACCESSIBLE_TO_THE_QUERY_PROCESSOR
              .toLocalizedString(new Object[] { _name,
                  target.getClass().getName() }));
    }
  }

  private void updateClassToFieldsMap(String className, String field) {
    Map<String, Set<String>> map = DefaultQuery.getPdxClasstofieldsmap();
    Set<String> fields = map.get(className);
    if (fields == null) {
      fields = new HashSet<String>();
      map.put(className, fields);
    }
    fields.add(field);
  }

  private boolean isFieldAlreadySearchedAndNotFound(String className, String field) {
    Set<String> fields = DefaultQuery.getPdxClasstofieldsmap().get(className);
    if (fields != null) {
      return fields.contains(field);
    }
    return false;
  }
  
  private void updateClassToMethodsMap(String className, String field) {
    Map<String, Set<String>> map = DefaultQuery.getPdxClasstoMethodsmap();
    Set<String> fields = map.get(className);
    if (fields == null) {
      fields = new HashSet<String>();
      map.put(className, fields);
    }
    fields.add(field);
  }

  private boolean isMethodAlreadySearchedAndNotFound(String className, String field) {
    Set<String> fields = DefaultQuery.getPdxClasstoMethodsmap().get(className);
    if (fields != null) {
      return fields.contains(field);
    }
    return false;
  }
  

  
}

