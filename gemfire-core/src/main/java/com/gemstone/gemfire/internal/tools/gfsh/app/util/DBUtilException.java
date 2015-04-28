/*=========================================================================
 * Copyright (c) 2008, Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */
package com.gemstone.gemfire.internal.tools.gfsh.app.util;

/**
 * DBUtilException is thrown by DBUtil when it encounters DB errors.
 * 
 * @author dpark
 *
 */
public class DBUtilException extends Exception
{
	private static final long serialVersionUID = 1L;
	
	public static final int ERROR_CONNECTION_CLOSED = 1;
    public static final int ERROR_CONNECTION_ALREADY_ESTABLISHED = 2;
    public static final int ERROR_NO_MATCHING_METHOD_FOR_COLUMN = 3;
    public static final int ERROR_UNDEFINED = -1;

    private int errorCode = ERROR_UNDEFINED;

    public DBUtilException()
    {
    }

    public DBUtilException(int errorCode, String message)
    {
        super(message);
        this.errorCode = errorCode;
    }

    public DBUtilException(String message, Throwable cause)
    {
        super(cause.getClass() + ": " + cause.getMessage() + ". " + message);
        initCause(cause);
    }

    public DBUtilException(Throwable cause)
    {
        super(cause.getClass() + ": " + cause.getMessage() + ".");
        initCause(cause);
    }
}
