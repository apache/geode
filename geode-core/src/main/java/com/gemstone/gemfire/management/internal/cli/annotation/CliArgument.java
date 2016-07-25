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
package com.gemstone.gemfire.management.internal.cli.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.shell.core.Converter;
import org.springframework.shell.core.annotation.CliCommand;

/**
 * Annotation for Argument of a Command
 * 
 * @since GemFire 7.0
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface CliArgument{

	/**
	 * @return name of the argument, useful during help and warning messages
	 */
	String name();

	/**
	 * @return a help message for this option (the default is a blank String,
	 *         which means there is no help)
	 */
	String help() default "";

	/**
	 * @return true if this argument must be specified one way or the other by
	 *         the user (defaults to false)
	 */
	boolean mandatory() default false;

	/**
	 * Returns a string providing context-specific information (e.g. a
	 * comma-delimited set of keywords) to the {@link Converter} that handles
	 * the annotated parameter's type.
	 * <p>
	 * For example, if a method parameter "thing" of type "Thing" is annotated
	 * as follows:
	 * 
	 * <pre>
	 * <code>@CliArgument(..., argumentContext = "foo,bar", ...) Thing thing</code>
	 * </pre>
	 * 
	 * ... then the {@link Converter} that converts the text entered by the user
	 * into an instance of Thing will be passed "foo,bar" as the value of the
	 * <code>optionContext</code> parameter in its public methods. This allows
	 * the behaviour of that Converter to be individually customised for each
	 * {@link CliArgument} of each {@link CliCommand}.
	 * 
	 * @return a non-<code>null</code> string (can be empty)
	 */
	String argumentContext() default "";

	/**
	 * @return if true, the user cannot specify this option and it is provided
	 *         by the shell infrastructure (defaults to false)
	 */
	boolean systemProvided() default false;

	/**
	 * @return the default value to use if this argument is unspecified by the
	 *         user (defaults to __NULL__, which causes null to be presented to
	 *         any non-primitive parameter)
	 */
	String unspecifiedDefaultValue() default "__NULL__";
}
