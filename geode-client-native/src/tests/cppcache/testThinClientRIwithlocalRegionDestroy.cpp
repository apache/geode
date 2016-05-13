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
  testRegisterAllKeysForLocalRegionDestroy( true, true); /*pool + locator*/
  testRegisterAllKeysForLocalRegionDestroy( true, false); /*pool with endpoint*/

  /*test registerKeys(vectorofKeys) behaviour for localDestroyRegion*/
  testRegisterKeyForLocalRegionDestroy(true, true); 
  testRegisterKeyForLocalRegionDestroy(true, false);

  /*test registerRegex() behaviour for localDestroyRegion*/
  testRegisterRegexForLocalRegionDestroy( true, true);
  testRegisterRegexForLocalRegionDestroy( true, false);

  /*test parent-subRegion behaviour in case of localDestroy*/
  testSubregionForLocalRegionDestroy( true, true);
  testSubregionForLocalRegionDestroy( true, false);
}
END_MAIN
