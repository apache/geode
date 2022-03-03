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
package org.apache.geode.management.internal;

import java.io.InvalidObjectException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeDataView;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;

/**
 * Open type converter for Composite type
 *
 *
 */

public class CompositeConverter extends OpenTypeConverter {

  private final String[] itemNames;
  private final Method[] getters;
  private final OpenTypeConverter[] getterConverters;
  private CompositeBuilder compositeBuilder;

  CompositeConverter(Class targetClass, CompositeType compositeType, String[] itemNames,
      Method[] getters) throws OpenDataException {
    super(targetClass, compositeType, CompositeData.class);

    assert (itemNames.length == getters.length);

    this.itemNames = itemNames;
    this.getters = getters;
    getterConverters = new OpenTypeConverter[getters.length];
    for (int i = 0; i < getters.length; i++) {
      Type retType = getters[i].getGenericReturnType();
      getterConverters[i] = OpenTypeConverter.toConverter(retType);
    }
  }

  /**
   * Converts to open value
   */
  @Override
  Object toNonNullOpenValue(Object value) throws OpenDataException {
    CompositeType ct = (CompositeType) getOpenType();
    if (value instanceof CompositeDataView) {
      return ((CompositeDataView) value).toCompositeData(ct);
    }
    if (value == null) {
      return null;
    }

    Object[] values = new Object[getters.length];
    for (int i = 0; i < getters.length; i++) {
      try {
        Object got = getters[i].invoke(value, (Object[]) null);
        values[i] = getterConverters[i].toOpenValue(got);
      } catch (Exception e) {
        throw openDataException("Error calling getter for " + itemNames[i] + ": " + e, e);
      }
    }
    return new CompositeDataSupport(ct, itemNames, values);
  }

  /**
   * Determine how to convert back from the CompositeData into the original Java type. For a type
   * that is not reconstructible, this method will fail every time, and will throw the right
   * exception.
   */
  private synchronized void makeCompositeBuilder() throws InvalidObjectException {
    if (compositeBuilder != null) {
      return;
    }

    Class targetClass = (Class<?>) getTargetType();

    CompositeBuilder[][] builders = {{new CompositeBuilderViaFrom(targetClass, itemNames),},
        {new CompositeBuilderViaConstructor(targetClass, itemNames),},
        {new CompositeBuilderCheckGetters(targetClass, itemNames, getterConverters),
            new CompositeBuilderViaSetters(targetClass, itemNames),
            new CompositeBuilderViaProxy(targetClass, itemNames),},};
    CompositeBuilder foundBuilder = null;

    StringBuilder whyNots = new StringBuilder();
    Throwable possibleCause = null;
    find: for (CompositeBuilder[] relatedBuilders : builders) {
      for (int i = 0; i < relatedBuilders.length; i++) {
        CompositeBuilder builder = relatedBuilders[i];
        String whyNot = builder.applicable(getters);
        if (whyNot == null) {
          foundBuilder = builder;
          break find;
        }
        Throwable cause = builder.possibleCause();
        if (cause != null) {
          possibleCause = cause;
        }
        if (whyNot.length() > 0) {
          if (whyNots.length() > 0) {
            whyNots.append("; ");
          }
          whyNots.append(whyNot);
          if (i == 0) {
            break;
          }
        }
      }
    }
    if (foundBuilder == null) {
      String msg = "Do not know how to make a " + targetClass.getName() + " from a CompositeData: "
          + whyNots;
      if (possibleCause != null) {
        msg += ". Remaining exceptions show a POSSIBLE cause.";
      }
      throw invalidObjectException(msg, possibleCause);
    }
    compositeBuilder = foundBuilder;
  }

  @Override
  void checkReconstructible() throws InvalidObjectException {
    makeCompositeBuilder();
  }

  @Override
  public Object fromNonNullOpenValue(Object value) throws InvalidObjectException {
    makeCompositeBuilder();
    return compositeBuilder.fromCompositeData((CompositeData) value, itemNames, getterConverters);
  }

}
