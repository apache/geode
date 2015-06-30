package com.gemstone.gemfire.modules.session.installer.args;

/**
 * Interface defining unknown argument handlers, given the opportunity to either
 * ignore the issue or force the parameter to be dealt with.
 */
public interface UnknownArgumentHandler {

  /**
   * Called when an unknown argument is supplied.
   *
   * @param form   argument name used
   * @param params parameters passed into it
   * @throws UsageException when the user needs to fix it
   */
  void handleUnknownArgument(String form, String[] params)
      throws UsageException;

}
