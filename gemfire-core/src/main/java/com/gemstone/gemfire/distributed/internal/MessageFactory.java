/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
