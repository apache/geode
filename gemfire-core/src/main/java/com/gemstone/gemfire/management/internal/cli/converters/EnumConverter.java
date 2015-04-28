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
 * {@link Converter} for {@link Enum}.
 *
 * @author Abhishek Chaudhari
 * @since 7.0
 */
/*
 * Use this EnumConverter instead of SHL's EnumConverter. Added null check for
 * existingData in getAllPossibleValues 
 * 
 * Original authors: Ben Alex & Alan Stewart
 */
@SuppressWarnings("all") // Enum parameter warning
public class EnumConverter implements Converter<Enum> {

	public Enum convertFromText(final String value, final Class<?> requiredType, final String optionContext) {
		Class<Enum> enumClass = (Class<Enum>) requiredType;
		return Enum.valueOf(enumClass, value);
	}

	public boolean getAllPossibleValues(final List<Completion> completions, 
	    final Class<?> requiredType, final String existingData, 
	    final String optionContext, final MethodTarget target) {
		Class<Enum> enumClass = (Class<Enum>) requiredType;
		for (Enum enumValue : enumClass.getEnumConstants()) {
			String candidate = enumValue.name();
      // GemFire/gfsh addition - check 'existingData == null'. GfshParser can 
			// pass existingData as null  
      if ("".equals(existingData) || existingData == null
          || candidate.startsWith(existingData)
          || existingData.startsWith(candidate)
          || candidate.toUpperCase().startsWith(existingData.toUpperCase())
          || existingData.toUpperCase().startsWith(candidate.toUpperCase())) {
				completions.add(new Completion(candidate));
			}
		}
		return true;
	}

	public boolean supports(final Class<?> requiredType, final String optionContext) {
		return Enum.class.isAssignableFrom(requiredType);
	}
}
