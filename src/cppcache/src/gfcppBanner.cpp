#include "gfcppBanner.hpp"
using namespace gemfire;
std::string gfcppBanner::getBanner() {
  std::string str =
      ""
      " -----------------------------------------------------------------------"
      "----\n"
      " Copyright © 1997-2014 Pivotal Software, Inc. All Rights Reserved. This "
      "product is\n"
      " protected by U.S. and international copyright and intellectual "
      "property\n"
      " laws. Pivotal products are covered by one or more patents listed at \n"
      " http://www.pivotal.io/patents.  Pivotal is a registered trademark or\n"
      " trademark of Pivotal Software, Inc. in the United States and/or other "
      "jurisdictions.\n"
      " All other marks and names mentioned herein may be trademarks of their "
      "\n"
      " respective companies.\n"
      " -----------------------------------------------------------------------"
      "----\n";
  return str;
}
