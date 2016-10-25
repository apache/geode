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
package org.apache.geode.internal;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Utility methods for managing and using Sets 
 * @since GemFire 59poc
 * 
 */
public final class SetUtils {
  public static  <T> boolean intersectsWith(final Set<? extends T> a, final Set<? extends T> b) {
    if (a == b) {
      return true;
    }
    final Set/*<T>*/ lSet, sSet;
    if (a.size() >= b.size()) {
      lSet = a; sSet = b;
    } else {
      lSet = b; sSet = a;
    }
    for (Iterator i=sSet.iterator(); i.hasNext(); ) {
      Object item = i.next();
      if (lSet.contains(item)) {
        return true;
      }
    }
    return false;
  }
  
  public static /*T*/ Set/*<T>*/ intersection(final Set/*<T>*/ a, final Set/*<T>*/ b) {
    if (a == b) {
      return a;
    }
    final Set/*<T>*/ lSet, sSet;
    if (a.size() >= b.size()) {
      lSet = a; sSet = b;
    } else {
      lSet = b; sSet = a;
    }
    HashSet /*<T>*/ ret = new HashSet/*<T>*/();
    for (Iterator i=sSet.iterator(); i.hasNext(); ) {
      Object item = i.next();
      if (lSet.contains(item)) {
        ret.add(item);
      }
    }
    return ret;
  }
}
