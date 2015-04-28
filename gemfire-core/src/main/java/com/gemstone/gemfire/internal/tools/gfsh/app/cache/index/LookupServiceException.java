package com.gemstone.gemfire.internal.tools.gfsh.app.cache.index;

/**
 * LookupService throws LookupServiceException if it encounters
 * an error from the underlying GemFire communications mechanism.
 */
class LookupServiceException extends RuntimeException
{
	private static final long serialVersionUID = 1L;

	/**
	 * Constructs a new LookupServiceException.
	 */
    LookupServiceException()
    {
        super();
    }

    /**
     * Constructs a new LookupServiceException exception with the specified detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     */
    public LookupServiceException(String message)
    {
        super(message, null);
    }

    /**
     * Constructs a new LookupServiceException exception with the specified detail message and
     * cause.
     * <p>Note that the detail message associated with
     * <code>cause</code> is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param  message the detail message (which is saved for later retrieval
     *         by the {@link #getMessage()} method).
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public LookupServiceException(String message, Throwable cause)
    {
        super(message, cause);
    }

    /**
     * Constructs a new LookupServiceException exception with the specified cause.
     * <p>Note that the detail message associated with
     * <code>cause</code> is <i>not</i> automatically incorporated in
     * this exception's detail message.
     *
     * @param  cause the cause (which is saved for later retrieval by the
     *         {@link #getCause()} method).  (A <tt>null</tt> value is
     *         permitted, and indicates that the cause is nonexistent or
     *         unknown.)
     */
    public LookupServiceException(Throwable cause)
    {
        super(cause);
    }
}
