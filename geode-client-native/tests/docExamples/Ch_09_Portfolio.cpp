/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Ch_09_Portfolio.hpp"
#include "CacheHelper.hpp"

int main(int argc, char* argv[])
{
  try {
    printf("\nCh_09_Portfolio EXAMPLES: Starting...");
    // Portfolio pf;
    printf("\nCh_09_Portfolio EXAMPLES: All Done.");
  }catch (const Exception & excp)
  {
    printf("\nEXAMPLES: %s: %s", excp.getName(), excp.getMessage());
    exit(1);
  }
  catch(...)
  {
    printf("\nEXAMPLES: Unknown exception");
    exit(1);
  }
  return 0;
}
