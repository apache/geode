#ifndef CPPCACHE_INTEGRATION_TEST_LOCATOR_GLOBALS_HPP_
#define CPPCACHE_INTEGRATION_TEST_LOCATOR_GLOBALS_HPP_

static int numberOfLocators = 1;
bool isLocalServer = false;
bool isLocator = false;
const char* locatorsG =
    CacheHelper::getLocatorHostPort(isLocator, isLocalServer, numberOfLocators);

#endif /* CPPCACHE_INTEGRATION_TEST_LOCATOR_GLOBALS_HPP_ */
