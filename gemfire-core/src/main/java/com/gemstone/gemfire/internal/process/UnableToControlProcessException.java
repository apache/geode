package com.gemstone.gemfire.internal.process;

/**
 * Exception indicating that an attempt to control a {@link ControllableProcess}
 * has failed for some reason.
 * 
 * @author Kirk Lund
 * @since 8.0
 */
public final class UnableToControlProcessException extends Exception {
  private static final long serialVersionUID = 7579463534993125290L;

  /**
   * Creates a new <code>UnableToControlProcessException</code>.
   */
  public UnableToControlProcessException(final String message) {
    super(message);
  }

  /**
   * Creates a new <code>UnableToControlProcessException</code> that was
   * caused by a given exception
   */
  public UnableToControlProcessException(final String message, final Throwable thr) {
    super(message, thr);
  }

  /**
   * Creates a new <code>UnableToControlProcessException</code> that was
   * caused by a given exception
   */
  public UnableToControlProcessException(final Throwable thr) {
    super(thr.getMessage(), thr);
  }
}
