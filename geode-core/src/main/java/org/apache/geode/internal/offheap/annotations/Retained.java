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
package org.apache.geode.internal.offheap.annotations;

import static org.apache.geode.internal.offheap.annotations.OffHeapIdentifier.*;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to mark a reference count increment to an off-heap value: 
 * <ul>
 * <li>When used on a method declaration it indicates that the method called retain on the return value if it is an off-heap reference.</li>
 * <li>When used on a constructor declaration this annotation indicates that field members may be off-heap references and retain will be invoked on the field methods.  This also indicates that the class will have a release method.</li>
 * <li>When used on a parameter declaration it indicates that the method will call retain on the parameter if it is an off-heap reference.</li> 
 * <li>When used on a local variable it indicates that the variable will reference an off-heap value that has been retained.  Typically, the method will also be responsible for releasing the value (unless it is the return value).</li>
 * <li>This annotation is also used to mark fields (instance variables) that will have a retain count during the lifetime of the containing object.  Typically, these fields will have their reference counts decremented in release method.</li>
 * </ul>
 * 
 * One or more OffHeapIdentifiers may be supplied if the developer wishes to link this annotation with other
 * off-heap annotations.
 * 
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.PARAMETER,
              ElementType.METHOD,
              ElementType.CONSTRUCTOR,
              ElementType.FIELD,
              ElementType.LOCAL_VARIABLE})
@Documented
public @interface Retained {
  OffHeapIdentifier[] value() default DEFAULT;
}
