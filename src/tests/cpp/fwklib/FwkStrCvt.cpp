/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "fwklib/FwkStrCvt.hpp"
#include "fwklib/FwkLog.hpp"

using namespace gemfire;
using namespace testframework;

// ----------------------------------------------------------------------------

static const short int wrd = 0x0001;
static const char* byt = reinterpret_cast<const char*>(&wrd);

#define isNetworkOrder() (byt[1] == 1)

#define NSWAP_8(x) ((x)&0xff)
#define NSWAP_16(x) ((NSWAP_8(x) << 8) | NSWAP_8((x) >> 8))
#define NSWAP_32(x) ((NSWAP_16(x) << 16) | NSWAP_16((x) >> 16))
#define NSWAP_64(x) ((NSWAP_32(x) << 32) | NSWAP_32((x) >> 32))

int64_t FwkStrCvt::hton64(int64_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_64(value);
}

int64_t FwkStrCvt::ntoh64(int64_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_64(value);
}

int32_t FwkStrCvt::hton32(int32_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_32(value);
}

int32_t FwkStrCvt::ntoh32(int32_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_32(value);
}

int16_t FwkStrCvt::hton16(int16_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_16(value);
}

int16_t FwkStrCvt::ntoh16(int16_t value) {
  if (isNetworkOrder()) return value;
  return NSWAP_16(value);
}

// ----------------------------------------------------------------------------

char* FwkStrCvt::hexify(uint8_t* buff, int32_t len) {
  char* obuff = new char[(len + 1) * 2];
  for (int32_t i = 0; i < len; i++) {
    sprintf((obuff + (i * 2)), "%02x", buff[i]);
  }
  return obuff;
}

// ----------------------------------------------------------------------------

std::string FwkStrCvt::toTimeString(int32_t seconds) {
  std::string result;
  const int32_t perMinute = 60;
  const int32_t perHour = 3600;
  const int32_t perDay = 86400;

  if (seconds <= 0) {
    result += "No limit specified.";
    return result;
  }

  int32_t val = 0;
  if (seconds > perDay) {
    val = seconds / perDay;
    if (val > 0) {
      result += FwkStrCvt(val).toString();
      if (val == 1) {
        result += " day ";
      } else {
        result += " days ";
      }
    }
    seconds %= perDay;
  }
  if (seconds > perHour) {
    val = seconds / perHour;
    if (val > 0) {
      result += FwkStrCvt(val).toString();
      if (val == 1) {
        result += " hour ";
      } else {
        result += " hours ";
      }
    }
    seconds %= perHour;
  }
  if (seconds > perMinute) {
    val = seconds / perMinute;
    if (val > 0) {
      result += FwkStrCvt(val).toString();
      if (val == 1) {
        result += " minute ";
      } else {
        result += " minutes ";
      }
    }
    seconds %= perMinute;
  }
  if (seconds > 0) {
    result += FwkStrCvt(seconds).toString();
    if (seconds == 1) {
      result += " second ";
    } else {
      result += " seconds ";
    }
  }
  return result;
}

// This expects a string of the form: 12h23m43s
// and returns the time encoded in the string as
// the integer number of seconds it represents.
int32_t FwkStrCvt::toSeconds(const char* str) {
  int32_t max = static_cast<int32_t>(ACE_OS::strspn(str, "0123456789hHmMsS"));
  std::string tstr(str, max);

  int32_t seconds = 0, i1 = 0, i2 = 0, i3 = 0;
  char c1 = 0, c2 = 0, c3 = 0;

  sscanf(tstr.c_str(), "%d%c%d%c%d%c", &i1, &c1, &i2, &c2, &i3, &c3);
  if (i1 > 0) seconds += asSeconds(i1, c1);
  if (i2 > 0) seconds += asSeconds(i2, c2);
  if (i3 > 0) seconds += asSeconds(i3, c3);
  return seconds;
}

int32_t FwkStrCvt::asSeconds(int32_t val, char typ) {
  int32_t ret = 0;
  switch (typ) {
    case 'm':
    case 'M':
      ret = val * 60;
      break;
    case 'h':
    case 'H':
      ret = val * 3600;
      break;
    default:
      ret = val;
      break;
  }
  return ret;
}
