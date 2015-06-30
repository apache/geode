package com.gemstone.gemfire.modules.session;

/**
 * Basic commands to pass to our test servlet
 */
public enum QueryCommand {

  SET,

  GET,

  INVALIDATE,

  CALLBACK,

  UNKNOWN;

}
