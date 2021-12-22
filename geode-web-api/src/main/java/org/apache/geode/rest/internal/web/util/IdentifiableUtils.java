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

package org.apache.geode.rest.internal.web.util;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.springframework.util.ClassUtils;
import org.springframework.util.MethodInvoker;
import org.springframework.util.ObjectUtils;

/**
 * The IdentifierUtils class is a utility class for working with Objects having identifiers
 * <p/>
 *
 * @since GemFire 8.0
 */
@SuppressWarnings("unused")
public abstract class IdentifiableUtils {

  private static final AtomicLong ID_SEQUENCE = new AtomicLong(0l);

  protected static final Logger LOGGER = Logger.getLogger(IdentifiableUtils.class.getName());

  public static synchronized long createId() {
    return ID_SEQUENCE.incrementAndGet();
  }

  public static synchronized Long createId(final Long baseId) {
    final long delta = ((baseId != null ? baseId : 0l) - ID_SEQUENCE.get());
    long newId = (delta > 0 ? ID_SEQUENCE.addAndGet(delta) : createId());
    return newId;
  }

  @SuppressWarnings("unchecked")
  public static <T> T getId(final Object identifiableObject) {
    if (isGetIdMethodAvailable(identifiableObject)) {
      final MethodInvoker method = new MethodInvoker();

      method.setTargetObject(identifiableObject);
      method.setTargetMethod("getId");

      try {
        return (T) method.invoke();
      } catch (Exception e) {
        LOGGER.log(Level.SEVERE, "Invocation failure.", e);
      }
    }

    return null;
  }

  public static boolean isGetIdMethodAvailable(final Object identifiableObject) {
    return (identifiableObject != null
        && ClassUtils.hasMethod(identifiableObject.getClass(), "getId"));
  }

  @SuppressWarnings("unchecked")
  public static <T> void setId(final Object identifiableObject, final T id) {

    if (isSetIdMethodAvailable(identifiableObject, id)) {
      final MethodInvoker method = new MethodInvoker();

      method.setTargetObject(identifiableObject);
      method.setTargetMethod("setId");
      method.setArguments(id);

      try {
        method.prepare();
        method.invoke();
      } catch (Exception e) {
        throw new IllegalArgumentException(
            String.format("Failed to set ID (%1$s) on Object (%2$s)!", id, identifiableObject), e);
      }
    } else {
      LOGGER.warning(String.format(
          "Not able to set ID (%1$s) of type (%2$s) on Object of type (%3$s)", id,
          ObjectUtils.nullSafeClassName(id), ObjectUtils.nullSafeClassName(identifiableObject)));
    }
  }

  public static <T> boolean isSetIdMethodAvailable(final Object identifiableObject, final T id) {
    return (identifiableObject != null && ClassUtils.hasMethod(identifiableObject.getClass(),
        "setId", (id == null ? Object.class : id.getClass())));
  }

}
