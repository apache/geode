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
package com.gemstone.gemfire.test.junit;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.gemstone.gemfire.test.junit.support.DefaultIgnoreCondition;

/**
 * The ConditionalIgnore class is a Java Annotation used to annotated a test suite class test case method in order to
 * conditionally ignore the test case for a fixed amount of time, or based on a predetermined condition provided by
 * the IgnoreCondition interface.
 *
 * @author John Blum
 * @see java.lang.annotation.Annotation
 * @see com.gemstone.gemfire.test.junit.IgnoreCondition
 * @see com.gemstone.gemfire.test.junit.support.DefaultIgnoreCondition
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.METHOD })
@SuppressWarnings("unused")
public @interface ConditionalIgnore {

  Class<? extends IgnoreCondition> condition() default DefaultIgnoreCondition.class;

  String until() default "1970-01-01";

  String value() default "";

}
