/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.rest.internal.web.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import org.springframework.util.ClassUtils;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;

/**
 * The IdentifierUtils class is a utility class for working with Objects having
 * identifiers
 * <p/>
 * 
 * @author John Blum, Nilkanth Patel
 * @since 8.0
 */
@SuppressWarnings("unused")
public abstract class IdentifiableUtils {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0l);

  protected static final Logger LOGGER = Logger
      .getLogger(IdentifiableUtils.class.getName());

  public static synchronized long createId() {
    return ID_SEQUENCE.incrementAndGet();
  }

  public static synchronized Long createId(final Long baseId) {
    final long delta = ((baseId != null ? baseId.longValue() : 0l) - ID_SEQUENCE.get()); 
    long newId = (delta > 0 ? ID_SEQUENCE.addAndGet(delta) : createId());
    return Long.valueOf(newId);
  }

  @SuppressWarnings("unchecked")
  public static <T> T getId(final Object identifiableObject) {
    if (isGetIdMethodAvailable(identifiableObject)) {
      final MethodInvoker method = new MethodInvoker();

      method.setTargetObject(identifiableObject);
      method.setTargetMethod("getId");

      try {
        return (T) method.invoke();
      } catch (Exception ignore) {
        ignore.printStackTrace();
      }
    }

    return null;
  }

  public static boolean isGetIdMethodAvailable(final Object identifiableObject) {
    return (identifiableObject != null && ClassUtils.hasMethod(
        identifiableObject.getClass(), "getId"));
  }

  @SuppressWarnings("unchecked")
  public static <T> void setId(final Object identifiableObject, final T id) {
    
    if (isSetIdMethodAvailable(identifiableObject, id)) {
      final MethodInvoker method = new MethodInvoker();

      method.setTargetObject(identifiableObject);
      method.setTargetMethod("setId");
      method.setArguments(new Object[] { id });

      try {
        method.prepare();
        method.invoke();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Failed to set ID (%1$s) on Object (%2$s)!", id,
                identifiableObject), e);
      }
    } else {
      LOGGER.warning(String.format(
          "Not able to set ID (%1$s) of type (%2$s) on Object of type (%3$s)",
          id, ObjectUtils.nullSafeClassName(id),
          ObjectUtils.nullSafeClassName(identifiableObject)));
    }
  }

  public static <T> boolean isSetIdMethodAvailable(
      final Object identifiableObject, final T id) {
    return (identifiableObject != null && ClassUtils.hasMethod(
        identifiableObject.getClass(), "setId",
        (id == null ? Object.class : id.getClass())));
  }

}
