/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "ThinClientRIwithlocalRegionDestroy.hpp"

DUNIT_MAIN
  {
    /*test registerAllKeys() behaviour for localDestroyRegion*/
    testRegisterAllKeysForLocalRegionDestroy();

    /*test registerKeys(vectorofKeys) behaviour for localDestroyRegion*/
    testRegisterKeyForLocalRegionDestroy();

    /*test registerRegex() behaviour for localDestroyRegion*/
    testRegisterRegexForLocalRegionDestroy();

    /*test parent-subRegion behaviour in case of localDestroy*/
    testSubregionForLocalRegionDestroy();
  }
END_MAIN
