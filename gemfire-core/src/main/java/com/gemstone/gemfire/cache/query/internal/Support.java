/*
*========================================================================
* Copyright Copyright (c) 2000-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
*========================================================================
*/

package com.gemstone.gemfire.cache.query.internal;

import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.InternalGemFireError;



public class Support
{
    public static final boolean ASSERTIONS_ENABLED = true;

    private static final int OTHER = 0;
    private static final int STATE = 1;
    private static final int ARG = 2;
    
    
    public static void assertArg(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, ARG);
    }

    public static void assertState(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, STATE);
    }
    
    
    public static void Assert(boolean b)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, "", OTHER);
    }

    public static void Assert(boolean b, String message)
    {
        if (!ASSERTIONS_ENABLED)
            return;
        Assert(b, message, OTHER);
    }
    
    public static void assertionFailed(String message)
    {
        assertionFailed(message, OTHER);
    }
    
    public static void assertionFailed()
    {
        assertionFailed("", OTHER);
    }
    
    private static void Assert(boolean b, String message, int type)
    {
        if (!b)
            assertionFailed(message, type);
    }    

    private static void assertionFailed(String message, int type)
    {
        switch (type)
        {
            case ARG:
                throw new IllegalArgumentException(message);
            case STATE:
                throw new IllegalStateException(message);
            default:
                throw new InternalGemFireError(LocalizedStrings.Support_ERROR_ASSERTION_FAILED_0.toLocalizedString(message));
        }
        
            // com.gemstone.persistence.jdo.GsRuntime.notifyCDebugger(null);
    }
    

}
