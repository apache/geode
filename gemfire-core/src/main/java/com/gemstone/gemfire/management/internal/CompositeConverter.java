/*
 *  =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *  ========================================================================
 */
package com.gemstone.gemfire.management.internal;

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
 * @author rishim
 * 
 */

public class CompositeConverter extends OpenTypeConverter {

  private final String[] itemNames;
  private final Method[] getters;
  private final OpenTypeConverter[] getterConverters;
  private CompositeBuilder compositeBuilder;

  CompositeConverter(Class targetClass, CompositeType compositeType,
      String[] itemNames, Method[] getters) throws OpenDataException {
    super(targetClass, compositeType, CompositeData.class);

    assert (itemNames.length == getters.length);

    this.itemNames = itemNames;
    this.getters = getters;
    this.getterConverters = new OpenTypeConverter[getters.length];
    for (int i = 0; i < getters.length; i++) {
      Type retType = getters[i].getGenericReturnType();
      getterConverters[i] = OpenTypeConverter.toConverter(retType);
    }
  }

  /**
   * Converts to open value
   */
  final Object toNonNullOpenValue(Object value) throws OpenDataException {
    CompositeType ct = (CompositeType) getOpenType();
    if (value instanceof CompositeDataView)
      return ((CompositeDataView) value).toCompositeData(ct);
    if (value == null)
      return null;

    Object[] values = new Object[getters.length];
    for (int i = 0; i < getters.length; i++) {
      try {
        Object got = getters[i].invoke(value, (Object[]) null);
        values[i] = getterConverters[i].toOpenValue(got);
      } catch (Exception e) {
        throw openDataException("Error calling getter for " + itemNames[i]
            + ": " + e, e);
      }
    }
    return new CompositeDataSupport(ct, itemNames, values);
  }

  /**
   * Determine how to convert back from the CompositeData into the original Java
   * type. For a type that is not reconstructible, this method will fail every
   * time, and will throw the right exception.
   */
  private synchronized void makeCompositeBuilder()
      throws InvalidObjectException {
    if (compositeBuilder != null)
      return;

    Class targetClass = (Class<?>) getTargetType();

    CompositeBuilder[][] builders = {
        { new CompositeBuilderViaFrom(targetClass, itemNames), },
        { new CompositeBuilderViaConstructor(targetClass, itemNames), },
        {
            new CompositeBuilderCheckGetters(targetClass, itemNames,
                getterConverters),
            new CompositeBuilderViaSetters(targetClass, itemNames),
            new CompositeBuilderViaProxy(targetClass, itemNames), }, };
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
        if (cause != null)
          possibleCause = cause;
        if (whyNot.length() > 0) {
          if (whyNots.length() > 0)
            whyNots.append("; ");
          whyNots.append(whyNot);
          if (i == 0)
            break;
        }
      }
    }
    if (foundBuilder == null) {
      String msg = "Do not know how to make a " + targetClass.getName()
          + " from a CompositeData: " + whyNots;
      if (possibleCause != null)
        msg += ". Remaining exceptions show a POSSIBLE cause.";
      throw invalidObjectException(msg, possibleCause);
    }
    compositeBuilder = foundBuilder;
  }

  void checkReconstructible() throws InvalidObjectException {
    makeCompositeBuilder();
  }

  public final Object fromNonNullOpenValue(Object value)
      throws InvalidObjectException {
    makeCompositeBuilder();
    return compositeBuilder.fromCompositeData((CompositeData) value, itemNames,
        getterConverters);
  }

}
