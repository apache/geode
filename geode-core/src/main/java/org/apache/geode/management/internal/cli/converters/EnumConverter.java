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
package org.apache.geode.management.internal.cli.converters;

import java.util.List;

import org.springframework.shell.core.Completion;
import org.springframework.shell.core.Converter;
import org.springframework.shell.core.MethodTarget;

/**
 * {@link Converter} for {@link Enum}.
 *
 * @since GemFire 7.0
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
