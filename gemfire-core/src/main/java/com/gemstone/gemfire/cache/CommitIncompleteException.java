package com.gemstone.gemfire.cache;

/**
 * Thrown when a commit fails to complete due to errors
 * @author Mitch Thomas
 * @since 5.7
 */
public class CommitIncompleteException extends TransactionException {
private static final long serialVersionUID = 1017741483744420800L;

  public CommitIncompleteException(String message) {
    super(message);
  }

}
