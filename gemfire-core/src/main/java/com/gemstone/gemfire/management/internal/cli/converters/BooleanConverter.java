/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * {@link Converter} for {@link Boolean}. Use this BooleanConverter instead of
 * SHL's BooleanConverter. Removed completion & conversion for values like 0, 1,
 * yes, no.
 *
 * @author Abhishek Chaudhari
 * @since 7.0
 */
public class BooleanConverter implements Converter<Boolean> {

  public boolean supports(final Class<?> requiredType, final String optionContext) {
    return Boolean.class.isAssignableFrom(requiredType) || boolean.class.isAssignableFrom(requiredType);
  }

	public Boolean convertFromText(final String value, final Class<?> requiredType, final String optionContext) {
		if ("true".equalsIgnoreCase(value)) {
			return true;
		} else if ("false".equalsIgnoreCase(value)) {
			return false;
		} else {
			throw new IllegalArgumentException("Cannot convert " + value + " to type Boolean.");
		}
	}

	public boolean getAllPossibleValues(final List<Completion> completions, final Class<?> requiredType, final String existingData, final String optionContext, final MethodTarget target) {
		completions.add(new Completion("true"));
		completions.add(new Completion("false"));
		return false;
	}
}