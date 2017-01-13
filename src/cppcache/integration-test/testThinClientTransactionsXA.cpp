#include "ThinClientTransactionsXA.hpp"

DUNIT_MAIN
  {
    runTransactionOps(true, true);
    runTransactionOps(true, true);

    runTransactionOps(true, false);
    runTransactionOps(true, false);

    runTransactionOps(true, true, true);
    runTransactionOps(true, true, true);

    runTransactionOps(true, false, true);
    runTransactionOps(true, false, true);
  }
END_MAIN
