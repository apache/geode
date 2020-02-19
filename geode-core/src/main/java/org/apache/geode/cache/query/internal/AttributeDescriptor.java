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

import static org.apache.geode.cache.query.security.RestrictedMethodAuthorizer.UNAUTHORIZED_STRING;

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

import org.apache.geode.annotations.internal.MakeNotStatic;
import org.apache.geode.cache.EntryDestroyedException;
import org.apache.geode.cache.query.NameNotFoundException;
import org.apache.geode.cache.query.QueryInvocationTargetException;
import org.apache.geode.cache.query.QueryService;
import org.apache.geode.cache.query.security.MethodInvocationAuthorizer;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.lang.JavaWorkarounds;
import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxSerializationException;
import org.apache.geode.pdx.internal.InternalPdxInstance;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;
import org.apache.geode.security.NotAuthorizedException;

/**
 * Utility for managing an attribute
 */
public class AttributeDescriptor {
  private final String _name;
  private final TypeRegistry _pdxRegistry;
  /** cache for remembering the correct Member for a class and attribute */
  @MakeNotStatic
  static final ConcurrentMap<List, Member> _localCache = new ConcurrentHashMap<>();

  public AttributeDescriptor(TypeRegistry pdxRegistry, String name) {
    _name = name;
    _pdxRegistry = pdxRegistry;
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

  public Object read(Object target, ExecutionContext executionContext)
      throws NameNotFoundException, QueryInvocationTargetException {
    if (target == null || target == QueryService.UNDEFINED) {
      return QueryService.UNDEFINED;
    }

    if (target instanceof InternalPdxInstance) {
      return readPdx((InternalPdxInstance) target, executionContext);
    }

    // for non pdx objects
    return readReflection(target, executionContext);
  }

  // used when the resolution of an attribute must be on a superclass
  // instead of the runtime class
  Object readReflection(Object target, ExecutionContext executionContext)
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
          Method method = (Method) m;
          MethodInvocationAuthorizer authorizer = executionContext.getMethodInvocationAuthorizer();

          // CQs are generally executed on individual events, so caching is just an overhead.
          if (executionContext.isCqQueryContext()) {
            if (!authorizer.authorize(method, target)) {
              throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
            }
          } else {
            // Try to use previous result so authorizer gets invoked only once per query.
            boolean authorizationResult;
            Boolean cachedResult = (Boolean) executionContext.cacheGet(method);

            if (cachedResult == null) {
              // First time, evaluate and cache result.
              authorizationResult = authorizer.authorize(method, target);
              executionContext.cachePut(method, authorizationResult);
            } else {
              // Use cached result.
              authorizationResult = cachedResult;
            }

            if (!authorizationResult) {
              throw new NotAuthorizedException(UNAUTHORIZED_STRING + method.getName());
            }
          }

          return method.invoke(target, (Object[]) null);
        } catch (EntryDestroyedException e) {
          // eat the Exception
          return QueryService.UNDEFINED;
        } catch (IllegalAccessException e) {
          throw new NameNotFoundException(
              String.format(
                  "Method ' %s ' in class ' %s ' is not accessible to the query processor",
                  m.getName(), target.getClass().getName()),
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
                  m.getName(), target.getClass().getName()),
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

  @SuppressWarnings("unchecked")
  Member getReadMember(Class targetClass) throws NameNotFoundException {
    // mapping: public field (same name), method (getAttribute()), method (attribute())
    List key = new ArrayList();
    key.add(targetClass);
    key.add(_name);

    Member m = JavaWorkarounds.computeIfAbsent(_localCache, key, k -> {
      Member member = getReadField(targetClass);
      return member == null ? getReadMethod(targetClass) : member;
    });

    if (m == null) {
      throw new NameNotFoundException(String.format(
          "No public attribute named ' %s ' was found in class %s", _name, targetClass.getName()));
    }

    // override security for nonpublic derived classes with public members
    ((AccessibleObject) m).setAccessible(true);
    return m;
  }

  Field getReadField(Class targetType) {
    try {
      return targetType.getField(_name);
    } catch (NoSuchFieldException e) {
      return null;
    }
  }

  Method getReadMethod(Class targetType) {
    Method m;
    // Check for a getter method for this _name
    String beanMethod = "get" + _name.substring(0, 1).toUpperCase() + _name.substring(1);
    m = getReadMethod(targetType, beanMethod);

    if (m != null)
      return m;

    return getReadMethod(targetType, _name);
  }

  @SuppressWarnings("unchecked")
  Method getReadMethod(Class targetType, String methodName) {
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
  private Object readPdx(InternalPdxInstance pdxInstance, ExecutionContext executionContext)
      throws NameNotFoundException, QueryInvocationTargetException {
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
        PdxType pdxType = _pdxRegistry.getPdxTypeForField(_name, className);
        if (pdxType == null) {
          // remember the field that is not present in any version to avoid
          // trips to the registry next time
          updateClassToFieldsMap(className, _name);
        } else {
          return pdxType.getPdxField(_name).getFieldType().getDefaultValue();
        }
      }
      // if the field is not present in any of the versions try to
      // invoke implicit method call
      if (!this.isMethodAlreadySearchedAndNotFound(className, _name)) {
        try {
          return readFieldFromDeserializedObject(pdxInstance, executionContext);
        } catch (NameNotFoundException ex) {
          updateClassToMethodsMap(pdxInstance.getClassName(), _name);
          throw ex;
        }
      } else
        return QueryService.UNDEFINED;
    }
  }

  private Object readFieldFromDeserializedObject(InternalPdxInstance pdxInstance,
      ExecutionContext executionContext)
      throws NameNotFoundException, QueryInvocationTargetException {
    Object obj;

    try {
      obj = pdxInstance.getCachedObject();
    } catch (PdxSerializationException e) {
      throw new NameNotFoundException( // the domain object is not available
          String.format("Field '%s' is not accessible to the query processor because: %s",
              _name, e.getMessage()));
    }

    return readReflection(obj, executionContext);
  }

  private void updateClassToFieldsMap(String className, String field) {
    Map<String, Set<String>> map = DefaultQuery.getPdxClasstofieldsmap();
    Set<String> fields = map.get(className);
    if (fields == null) {
      fields = new HashSet<>();
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
      fields = new HashSet<>();
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
