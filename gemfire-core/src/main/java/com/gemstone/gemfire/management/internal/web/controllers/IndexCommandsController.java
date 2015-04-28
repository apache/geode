/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;
import com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * The IndexCommandsController class implements the REST API calls for the Gfsh Index commands.
 * </p>
 * @author John Blum
 * @see com.gemstone.gemfire.management.internal.cli.commands.IndexCommands
 * @see com.gemstone.gemfire.management.internal.cli.util.CommandStringBuilder
 * @see com.gemstone.gemfire.management.internal.web.controllers.AbstractCommandsController
 * @see org.springframework.stereotype.Controller
 * @see org.springframework.web.bind.annotation.PathVariable
 * @see org.springframework.web.bind.annotation.RequestMapping
 * @see org.springframework.web.bind.annotation.RequestMethod
 * @see org.springframework.web.bind.annotation.RequestParam
 * @see org.springframework.web.bind.annotation.ResponseBody
 * @since 8.0
 */
@Controller("indexController")
@RequestMapping(AbstractCommandsController.REST_API_VERSION)
@SuppressWarnings("unused")
public class IndexCommandsController extends AbstractCommandsController {

  private static final String DEFAULT_INDEX_TYPE = "range";

  @RequestMapping(method = RequestMethod.GET, value = "/indexes")
  @ResponseBody
  public String listIndex(@RequestParam(value = CliStrings.LIST_INDEX__STATS, defaultValue = "false") final Boolean withStats) {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.LIST_INDEX);
    command.addOption(CliStrings.LIST_INDEX__STATS, String.valueOf(Boolean.TRUE.equals(withStats)));
    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/indexes")
  @ResponseBody
  public String createIndex(@RequestParam(CliStrings.CREATE_INDEX__NAME) final String name,
                            @RequestParam(CliStrings.CREATE_INDEX__EXPRESSION) final String expression,
                            @RequestParam(CliStrings.CREATE_INDEX__REGION) final String regionNamePath,
                            @RequestParam(value = CliStrings.CREATE_INDEX__GROUP, required = false) final String groupName,
                            @RequestParam(value = CliStrings.CREATE_INDEX__MEMBER, required = false) final String memberNameId,
                            @RequestParam(value = CliStrings.CREATE_INDEX__TYPE, defaultValue = DEFAULT_INDEX_TYPE) final String type)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_INDEX);

    command.addOption(CliStrings.CREATE_INDEX__NAME, name);
    command.addOption(CliStrings.CREATE_INDEX__EXPRESSION, expression);
    command.addOption(CliStrings.CREATE_INDEX__REGION, regionNamePath);
    command.addOption(CliStrings.CREATE_INDEX__TYPE, type);

    if (hasValue(groupName)) {
      command.addOption(CliStrings.CREATE_INDEX__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.CREATE_INDEX__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.POST, value = "/indexes", params = "op=create-defined")
  @ResponseBody
  public String createDefinedIndexes(@RequestParam(value = CliStrings.CREATE_DEFINED_INDEXES__GROUP, required = false) final String groupName,
                                     @RequestParam(value = CliStrings.CREATE_DEFINED_INDEXES__MEMBER, required = false) final String memberNameId)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.CREATE_DEFINED_INDEXES);

    if (hasValue(groupName)) {
      command.addOption(CliStrings.CREATE_DEFINED_INDEXES__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.CREATE_DEFINED_INDEXES__MEMBER, memberNameId);
    }

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.DELETE, value = "/indexes", params = "op=clear-defined")
  @ResponseBody
  public String clearDefinedIndexes() {
    return processCommand(CliStrings.CLEAR_DEFINED_INDEXES);
  }

  @RequestMapping(method = RequestMethod.POST, value = "/indexes", params = "op=define")
  @ResponseBody
  public String defineIndex(@RequestParam(CliStrings.DEFINE_INDEX_NAME) final String name,
                            @RequestParam(CliStrings.DEFINE_INDEX__EXPRESSION) final String expression,
                            @RequestParam(CliStrings.DEFINE_INDEX__REGION) final String regionNamePath,
                            @RequestParam(value = CliStrings.DEFINE_INDEX__TYPE, defaultValue = DEFAULT_INDEX_TYPE) final String type)
  {
    CommandStringBuilder command = new CommandStringBuilder(CliStrings.DEFINE_INDEX);

    command.addOption(CliStrings.DEFINE_INDEX_NAME, name);
    command.addOption(CliStrings.DEFINE_INDEX__EXPRESSION, expression);
    command.addOption(CliStrings.DEFINE_INDEX__REGION, regionNamePath);
    command.addOption(CliStrings.DEFINE_INDEX__TYPE, type);

    return processCommand(command.toString());
  }

  @RequestMapping(method = RequestMethod.DELETE, value = "/indexes")
  @ResponseBody
  public String destroyIndexes(@RequestParam(value = CliStrings.DESTROY_INDEX__GROUP, required = false) final String groupName,
                               @RequestParam(value = CliStrings.DESTROY_INDEX__MEMBER, required = false) final String memberNameId,
                               @RequestParam(value = CliStrings.DESTROY_INDEX__REGION, required = false) final String regionNamePath)
  {
    return internalDestroyIndex(null, groupName, memberNameId, regionNamePath);
  }

  @RequestMapping(method = RequestMethod.DELETE, value = "/indexes/{name}")
  @ResponseBody
  public String destroyIndex(@PathVariable("name") final String indexName,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__GROUP, required = false) final String groupName,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__MEMBER, required = false) final String memberNameId,
                             @RequestParam(value = CliStrings.DESTROY_INDEX__REGION, required = false) final String regionNamePath)
  {
    return internalDestroyIndex(decode(indexName), groupName, memberNameId, regionNamePath);
  }

  protected String internalDestroyIndex(final String indexName,
                                        final String groupName,
                                        final String memberNameId,
                                        final String regionNamePath)
  {
    final CommandStringBuilder command = new CommandStringBuilder(CliStrings.DESTROY_INDEX);

    if (hasValue(indexName)) {
      command.addOption(CliStrings.DESTROY_INDEX__NAME, indexName);
    }

    if (hasValue(groupName)) {
      command.addOption(CliStrings.DESTROY_INDEX__GROUP, groupName);
    }

    if (hasValue(memberNameId)) {
      command.addOption(CliStrings.DESTROY_INDEX__MEMBER, memberNameId);
    }

    if (hasValue(regionNamePath)) {
      command.addOption(CliStrings.DESTROY_INDEX__REGION, regionNamePath);
    }

    return processCommand(command.toString());
  }

}
