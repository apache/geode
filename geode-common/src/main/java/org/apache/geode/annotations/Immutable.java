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

package org.apache.geode.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;
import java.util.Collections;
import java.util.List;

/**
 * Annotates a class that cannot be changed after construction or a field that cannot
 * be modified after initialization.
 *
 * This is broadly similar to the Java Concurrency in Practice annotation, except
 * that it can also be applied to a field to mark fields as immutable - for example
 * a List field that is initialized using {@link Collections#unmodifiableList(List)};
 */
@Documented
@Target({ElementType.TYPE, ElementType.FIELD})
public @interface Immutable {

  /** Optional description */
  String value() default "";
}
