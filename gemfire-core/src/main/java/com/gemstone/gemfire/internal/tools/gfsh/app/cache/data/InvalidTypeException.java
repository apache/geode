package com.gemstone.gemfire.internal.tools.gfsh.app.cache.data;

/**
 * Thrown when the object type is invalid.
 */
public class InvalidTypeException extends Exception
{
    /**
     * Constructs an <code>InvalidTypeException</code> with <code>null</code>
     * as its error detail message.
     */
    public InvalidTypeException() {
	    super();
    }

    /**
     * Constructs an <code>InvalidTypeException</code> with the specified detail
     * message. The error message string <code>s</code> can later be
     * retrieved by the <code>{@link java.lang.Throwable#getMessage}</code>
     * method of class <code>java.lang.Throwable</code>.
     *
     * @param   s   the detail message.
     */
    public InvalidTypeException(String s) {
	    super(s);

    }
}