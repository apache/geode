package com.gemstone.gemfire.management.internal.cli.exceptions;

import com.gemstone.gemfire.management.internal.cli.i18n.CliStrings;

public class IndexNotFoundException extends Exception {

  private static final long serialVersionUID = 1L;
  final String indexName;
  final String message;
  
  public IndexNotFoundException(final String indexName) {
    this.indexName = indexName;
    this.message = CliStrings.format("Index \" {0} \" not found", indexName);
  }
  
  public String getMessage() {
    return this.message;
  }
}
