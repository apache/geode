/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package hydra;

import com.gemstone.gemfire.*;

/**
 *  Abstract class for clients giving scheduling instructions to the
 *  hydra master.
 *  <p>
 *  Use the arguments to give the reason for the exception.
 */

public abstract class SchedulingOrder extends GemFireException {
    public SchedulingOrder() {
        super();
    }
    public SchedulingOrder(String s) {
        super(s);
    }
}
