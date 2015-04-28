/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.distributed.internal;

import com.gemstone.gemfire.InternalGemFireException;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;

/**
 * This class is a factory that creates instances
 * of {@link DistributionMessage}.
 * @deprecated use a constructor instead
 */
@Deprecated
public class MessageFactory {

  //////////////////////  Static Methods  //////////////////////

  /**
   * Returns a message of the given type.
   */
  public static DistributionMessage getMessage(Class messageType) {
    
//    DistributionMessage message;
    // create a new message
    try {
      Object o = messageType.newInstance();
      if (!(o instanceof DistributionMessage)) {
        throw new InternalGemFireException(LocalizedStrings.MessageFactory_CLASS_0_IS_NOT_A_DISTRIBUTIONMESSAGE.toLocalizedString(messageType.getName()));

      } else {
        // no need for further processing on the new message, so return it
        return (DistributionMessage)o;
      }

    } catch (InstantiationException ex) {
      throw new InternalGemFireException(LocalizedStrings.MessageFactory_AN_INSTANTIATIONEXCEPTION_WAS_THROWN_WHILE_INSTANTIATING_A_0.toLocalizedString(messageType.getName()), ex);

    } catch (IllegalAccessException ex) {
      throw new InternalGemFireException(LocalizedStrings.MessageFactory_COULD_NOT_ACCESS_ZEROARG_CONSTRUCTOR_OF_0.toLocalizedString(messageType.getName()), ex);
    }
  }
}
