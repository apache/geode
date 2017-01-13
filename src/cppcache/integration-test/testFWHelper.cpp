
#define ROOT_NAME "testFWHelper"

#include "fw_helper.hpp"

using test::cout;
using test::endl;

/**
 * @brief Test a test runs.
 */
BEGIN_TEST(TestOne)
  cout << "test 1." << endl;
END_TEST(TestOne)

/**
 * @brief Test a test runs.
 */
BEGIN_TEST(TestTwo)
  cout << "test 2." << endl;
END_TEST(TestTwo)
