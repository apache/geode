/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
import org.apache.geode.internal.cache.Token;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.FieldNotFoundInPdxVersion;
import org.apache.geode.pdx.internal.PdxInstanceImpl;

/**
 * Utility for managing an attribute
 *
 * @version $Revision: 1.1 $
 */


public class AttributeDescriptor {
  private final String _name;
  private final MethodInvocationAuthorizer _methodInvocationAuthorizer;
  /** cache for remembering the correct Member for a class and attribute */
  private static final ConcurrentMap<List, Member> _localCache = new ConcurrentHashMap();



  public AttributeDescriptor(MethodInvocationAuthorizer methodInvocationAuthorizer, String name) {
    _methodInvocationAuthorizer = methodInvocationAuthorizer;
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

  public Object read(Object target) throws NameNotFoundException, QueryInvocationTargetException {
    if (target == null || target == QueryService.UNDEFINED) {
      return QueryService.UNDEFINED;
    }
    if (target instanceof PdxInstance) {
      return readPdx((PdxInstance) target);
    }
    // for non pdx objects
    return readReflection(target);
  }

  // used when the resolution of an attribute must be on a superclass
  // instead of the runtime class
  private Object readReflection(Object target)
      throws NameNotFoundException, QueryInvocationTargetException {
    Support.Assert(target != null);
    Support.Assert(target != QueryService.UNDEFINED);
    if (target instanceof Token) {
      return QueryService.UNDEFINED;
    }

    Class resolutionClass = target.getClass();
    Member m = getReadMember(resolutionClass);
    try {
      if (m instanceof Method) {
        try {
          _methodInvocationAuthorizer.authorizeMethodInvocation((Method) m, target);
          return ((Method) m).invoke(target, (Object[]) null);
        } catch (EntryDestroyedException e) {
          // eat the Exception
          return QueryService.UNDEFINED;
        } catch (IllegalAccessException e) {
          throw new NameNotFoundException(
              String.format(
                  "Method ' %s ' in class ' %s ' is not accessible to the query processor",
                  new Object[] {m.getName(), target.getClass().getName()}),
              e);
        } catch (InvocationTargetException e) {
          // if the target exception is Exception, wrap that,
          // otherwise wrap the InvocationTargetException itself
          Throwable t = e.getTargetException();
          if ((t instanceof EntryDestroyedException)) {
            // eat the exception
            return QueryService.UNDEFINED;
          }
          if (t instanceof Exception)
            throw new QueryInvocationTargetException(t);
          throw new QueryInvocationTargetException(e);
        }
      } else {
        try {
          return ((Field) m).get(target);
        } catch (IllegalAccessException e) {
          throw new NameNotFoundException(
              String.format(
                  "Field ' %s ' in class ' %s ' is not accessible to the query processor",
                  new Object[] {m.getName(), target.getClass().getName()}),
              e);
        } catch (EntryDestroyedException e) {
          return QueryService.UNDEFINED;
        }
      }
    } catch (EntryDestroyedException e) {
      // eat the exception
      return QueryService.UNDEFINED;
    }
  }

  Member getReadMember(Class targetClass) throws NameNotFoundException {

    // mapping: public field (same name), method (getAttribute()),
    // method (attribute())
    List key = new ArrayList();
    key.add(targetClass);
    key.add(_name);

    Member m = _localCache.computeIfAbsent(key, k -> {
      Member member = getReadField(targetClass);
      return member == null ? getReadMethod(targetClass) : member;
    });

    if (m == null) {
      throw new NameNotFoundException(
          String.format("No public attribute named ' %s ' was found in class %s",
              new Object[] {_name, targetClass.getName()}));
    }

    // override security for nonpublic derived classes with public members
    ((AccessibleObject) m).setAccessible(true);
    return m;
  }


  private Field getReadField(Class targetType) {
    try {
      return targetType.getField(_name);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }



  private Method getReadMethod(Class targetType) {
    Method m;
    // Check for a getter method for this _name
    String beanMethod = "get" + _name.substring(0, 1).toUpperCase() + _name.substring(1);
    m = getReadMethod(targetType, beanMethod);

    if (m != null)
      return m;

    return getReadMethod(targetType, _name);
  }



  private Method getReadMethod(Class targetType, String methodName) {
    try {
      return targetType.getMethod(methodName, (Class[]) null);
    } catch (NoSuchMethodException e) {
      updateClassToMethodsMap(targetType.getCanonicalName(), _name);
      return null;
    }
  }

  /**
   * reads field value from a PdxInstance
   *
   * @return the value of the field from PdxInstance
   */
  private Object readPdx(PdxInstance target)
      throws NameNotFoundException, QueryInvocationTargetException {
    if (target instanceof PdxInstanceImpl) {
      PdxInstanceImpl pdxInstance = (PdxInstanceImpl) target;
      // if the field is present in the pdxinstance
      if (pdxInstance.hasField(_name)) {
        // return PdxString if field is a String otherwise invoke readField
        return pdxInstance.getRawField(_name);
      } else {
        // field not found in the pdx instance, look for the field in any of the
        // PdxTypes (versions of the pdxinstance) in the type registry
        String className = pdxInstance.getClassName();

        // don't look further for field or method or reflect on GemFire JSON data
        if (className.equals(JSONFormatter.JSON_CLASSNAME)) {
          return QueryService.UNDEFINED;
        }


        // check if the field was not found previously
        if (!isFieldAlreadySearchedAndNotFound(className, _name)) {
          try {
            return pdxInstance.getDefaultValueIfFieldExistsInAnyPdxVersions(_name, className);
          } catch (FieldNotFoundInPdxVersion e1) {
            // remember the field that is not present in any version to avoid
            // trips to the registry next time
            updateClassToFieldsMap(className, _name);
          }
        }
        // if the field is not present in any of the versions try to
        // invoke implicit method call
        if (!this.isMethodAlreadySearchedAndNotFound(className, _name)) {
          try {
            return readFieldFromDeserializedObject(pdxInstance, target);
          } catch (NameNotFoundException ex) {
            updateClassToMethodsMap(pdxInstance.getClassName(), _name);
            throw ex;
          }
        } else
          return QueryService.UNDEFINED;
      }
    } else {
      // target could be another implementation of PdxInstance like
      // PdxInstanceEnum, in this case getRawField and getCachedOjects methods are
      // not available
      if (((PdxInstance) target).hasField(_name)) {
        return ((PdxInstance) target).getField(_name);
      }
      throw new NameNotFoundException(
          String.format("Field ' %s ' in class ' %s ' is not accessible to the query processor",
              new Object[] {_name, target.getClass().getName()}));
    }
  }

  private Object readFieldFromDeserializedObject(PdxInstanceImpl pdxInstance, Object target)
      throws NameNotFoundException, QueryInvocationTargetException {
    try {
      Object obj = pdxInstance.getCachedObject();
      return readReflection(obj);
    } catch (PdxSerializationException e) {
      throw new NameNotFoundException( // the domain object is not available
          String.format("Field ' %s ' in class ' %s ' is not accessible to the query processor",
              new Object[] {_name, target.getClass().getName()}));
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
