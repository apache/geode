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

import com.gemstone.gemfire.management.internal.cli.commands.GfshHelpCommands;
import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.shell.Gfsh;

/**
 * {@link Converter} for {@link GfshHelpCommands#obtainHelp(String)}
 * 
 * @author Nikhil Jadhav
 *
 * @since 7.0
 */
public class HelpConverter implements Converter<String> {

  @Override
  public String convertFromText(String existingData, Class<?> dataType,
      String optionContext) {
    
    if (optionContext.equals(CliStrings.PARAM_CONTEXT_HELP)) {
      return existingData.replaceAll("\"", "").replaceAll("'", "");
    } else {
      return null;
    }
  }

  @Override
  public boolean getAllPossibleValues(List<Completion> completionCandidates,
      Class<?> dataType, String existingData, String optionContext,
      MethodTarget arg4) {

    List<String> commandNames = Gfsh.getCurrentInstance().obtainHelpCommandNames(existingData);
    
    for (String string : commandNames) {
      completionCandidates.add(new Completion(string));
    }
    if (completionCandidates.size() > 0) {
      return true;
    }
    return false;
  }

  @Override
  public boolean supports(Class<?> arg0, String optionContext) {
    if (String.class.isAssignableFrom(arg0)
        && optionContext.equals(CliStrings.PARAM_CONTEXT_HELP)) {
      return true;
    }
    return false;
  }
}
