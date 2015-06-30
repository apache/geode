package com.gemstone.gemfire.internal.cache;

/**
 * 
 * @author sbawaska
 */
public class RemoteTransactionCCEDUnitTest extends RemoteTransactionDUnitTest {

  private static final long serialVersionUID = 5960292521068781262L;

  public RemoteTransactionCCEDUnitTest(String name) {
    super(name);
  }
  
  @Override
  protected boolean getConcurrencyChecksEnabled() {
    return true;
  }
}
