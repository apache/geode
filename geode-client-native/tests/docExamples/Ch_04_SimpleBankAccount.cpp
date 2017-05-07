/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
#include "Ch_04_SimpleBankAccount.hpp"
#include "CacheHelper.hpp"

int main(int argc, char* argv[])
{
  try {
    printf("\nSimpleBankAccount EXAMPLES: Starting...");
    BankAccount sba(1,100);
    printf("\nBank Account Number is %d",sba.getAccount());
    printf("\nBank Account Owner is %d",sba.getOwner());
    printf("\nSimpleBankAccount EXAMPLES: All Done.");
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

