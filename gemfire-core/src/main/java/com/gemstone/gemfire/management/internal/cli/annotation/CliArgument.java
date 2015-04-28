/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
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
 * @author Nikhil Jadhav
 * @since 7.0
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
