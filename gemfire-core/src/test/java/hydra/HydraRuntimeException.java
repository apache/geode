/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package hydra;

import com.gemstone.gemfire.*;

public class HydraRuntimeException extends GemFireException {

    public HydraRuntimeException(String s) {
        super(s);
    }
    public HydraRuntimeException(String s,Exception e) {
        super(s,e);
    }
    public HydraRuntimeException(String s,Throwable t) {
        super(s,t);
    }
}
