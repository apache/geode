#include "InterestResultPolicy.hpp"

using namespace gemfire;

char InterestResultPolicy::nextOrdinal = 0;
InterestResultPolicy InterestResultPolicy::NONE;
InterestResultPolicy InterestResultPolicy::KEYS;
InterestResultPolicy InterestResultPolicy::KEYS_VALUES;
