package com.gemstone.gemfire.internal.tools.gfsh.app.pogo;

import com.gemstone.gemfire.GemFireException;

/**
 * InvalidKeyException is a runtime exception thrown if the key type 
 * is invalid. This can occur when the incorrect type value is put in
 * the message (MapLite) class.
 *   
 * @author dpark
 *
 */
public class InvalidKeyException extends GemFireException
{
	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new <code>InvalidKeyException</code> with the specified 
	 * message.
	 */
	public InvalidKeyException(String message)
	{
		super(message);
	}

	/**
	 * Creates a new <code>InvalidKeyException</code> wit the specified
	 * message and exception.
	 */
	public InvalidKeyException(String message, Throwable ex)
	{
		super(message, ex);
	}
}
