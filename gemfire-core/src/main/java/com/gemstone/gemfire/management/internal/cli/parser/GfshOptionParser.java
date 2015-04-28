/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import java.util.LinkedList;

import com.gemstone.gemfire.management.internal.cli.GfshParser;
import com.gemstone.gemfire.management.internal.cli.exceptions.CliException;

/**
 * Delegate used for parsing by {@link GfshParser}
 *  
 * @author Nikhil Jadhav
 * @since 7.0
 */
public interface GfshOptionParser {
	public void setArguments(LinkedList<Argument> arguments);

	public LinkedList<Argument> getArguments();

	public void setOptions(LinkedList<Option> options);

	public LinkedList<Option> getOptions();

	OptionSet parse(String userInput) throws CliException;
}