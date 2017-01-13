/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *
 * The specification of function behaviors is found in the corresponding .cpp
 *file.
 *
 *========================================================================
 */

#include "Utils.hpp"
#include "ace/OS.h"
#include "NanoTimer.hpp"
#include <ace/Recursive_Thread_Mutex.h>
#include <ace/INET_Addr.h>

extern "C" {
#include <stdio.h>
}

using namespace gemfire;

#ifdef _WIN32

pNew Utils::s_pNew = NULL;
pDelete Utils::s_pDelete = NULL;
bool Utils::s_setNewAndDelete = false;

void* operator new(size_t size) {
  if (!Utils::s_pNew) {
    gemfire::setDefaultNewAndDelete();
  }
  void* ret = Utils::s_pNew(size);
  if (ret == NULL) {
    throw gemfire::OutOfMemoryException(
        "Out of memory while executing operator new");
  }
  return ret;
}

void operator delete(void* p) { Utils::s_pDelete(p); }

void* operator new[](size_t size) { return operator new(size); }

void operator delete[](void* p) { operator delete(p); }

#endif  // _WIN32

int RandGen::operator()(size_t max) {
  unsigned int seed =
      static_cast<unsigned int>(NanoTimer::now());
  return ACE_OS::rand_r(&seed) % max;
}

int32_t Utils::getLastError() { return ACE_OS::last_error(); }
std::string Utils::getEnv(const char* varName) {
#ifdef _WIN32
  DWORD dwRet;
  char varValue[8192];
  dwRet = ::GetEnvironmentVariable(varName, varValue, 8192);
  if (dwRet == 0 && (::GetLastError() == ERROR_ENVVAR_NOT_FOUND)) {
    return "";
  }
  return varValue;
#else
  char* varValue = ACE_OS::getenv(varName);
  if (varValue == NULL) {
    return "";
  }
  return varValue;
#endif
}

void Utils::parseEndpointString(const char* endpoints, std::string& host,
                                uint16_t& port) {
  std::string endpointsStr(endpoints);
  LOGFINE("Parsing endpoint string [%s]", endpointsStr.c_str());
  // Parse this string to get all hostnames and port numbers.
  std::string endpoint;
  std::string::size_type length = endpointsStr.size();
  std::string::size_type pos = 0;
  pos = endpointsStr.find(':', 0);
  if (pos != std::string::npos) {
    endpoint = endpointsStr.substr(0, pos);
    pos += 1;  // skip ':'
    length -= (pos);
    endpointsStr = endpointsStr.substr(pos, length);
  } else {
    host = "";
    port = 0;
    return;
  }
  // trim white spaces.
  std::string::size_type wpos = endpoint.find_last_not_of(' ');
  if (wpos != std::string::npos) {
    endpoint.erase(wpos + 1);
    wpos = endpoint.find_first_not_of(' ');
    if (wpos != std::string::npos) endpoint.erase(0, wpos);
  }
  host = endpoint;
  port = atoi(endpointsStr.c_str());
}

/*
std::string
Utils::convertHostToCanonicalForm(const char* endpoint)
{
  ACE_INET_Addr aia;
  char canonical[MAXHOSTNAMELEN + 11] = {0};

  if ( endpoint == NULL ||
       strlen(endpoint) == 0 ) {
    LOGERROR("Cannot convert empty endpoint to canonical form");
    return "";
  }

  // first convert the incoming endpoint string to an inet addr

  if (aia.string_to_addr(endpoint) != 0) {
    LOGERROR("Could not convert endpoint [%s] to inet addr", endpoint);
    return endpoint;
  }

  // if its a loopback address, try to get our hostname

  if (aia.is_loopback()) {
    int port = atoi(strchr(endpoint, ':') + 1);
    if (ACE_OS::hostname(canonical, MAXHOSTNAMELEN) == 0) {
      struct hostent * host;
      if ( (host = ACE_OS::gethostbyname(canonical)) != NULL ) {
        if (h_errno != 0) {
          return endpoint;
        }
        sprintf(canonical, "%s:%d", host->h_name, port);
        if (aia.string_to_addr(canonical) != 0) {
          LOGERROR("Could not convert canonical endpoint [%s] to inet addr",
canonical);
          return endpoint;
        }
      }
    }
  }

  // convert first to FQDN host name, failing which try IP number

  if ( aia.addr_to_string(canonical, MAXHOSTNAMELEN + 10, 0) != 0 &&
       aia.addr_to_string(canonical, MAXHOSTNAMELEN + 10, 1) != 0 ) {
    LOGERROR("Could not convert [%s] from inet addr to canonical form",
endpoint);
    return endpoint;
  }

  return canonical;
}
*/

std::string Utils::convertHostToCanonicalForm(const char* endpoints) {
  if (endpoints == NULL) return NULL;
  std::string hostString("");
  uint16_t port = 0;
  std::string endpointsStr(endpoints);
  std::string endpointsStr1(endpoints);
  // Parse this string to get all hostnames and port numbers.
  std::string endpoint;
  std::string::size_type length = endpointsStr.size();
  std::string::size_type pos = 0;
  ACE_TCHAR hostName[256], fullName[256];
  pos = endpointsStr.find(':', 0);
  if (pos != std::string::npos) {
    endpoint = endpointsStr.substr(0, pos);
    pos += 1;  // skip ':'
    length -= (pos);
    endpointsStr = endpointsStr.substr(pos, length);
  } else {
    hostString = "";
    port = 0;
    return "";
  }
  hostString = endpoint;
  port = atoi(endpointsStr.c_str());
  if (strcmp(hostString.c_str(), "localhost") == 0) {
    ACE_OS::hostname(hostName, sizeof(hostName) - 1);
    struct hostent* host;
    host = ACE_OS::gethostbyname(hostName);
    if (host) {
      ACE_OS::snprintf(fullName, 256, "%s:%d", host->h_name, port);
      return fullName;
    }
  } else {
    pos = endpointsStr1.find('.', 0);
    if (pos != std::string::npos) {
      ACE_INET_Addr addr(endpoints);
      addr.get_host_name(hostName, 256);
      ACE_OS::snprintf(fullName, 256, "%s:%d", hostName, port);
      return fullName;
    }
  }
  return endpoints;
}

void Utils::parseEndpointNamesString(
    const char* endpoints, std::unordered_set<std::string>& endpointNames) {
  std::string endpointsStr(endpoints);
  // Parse this string to get all hostnames and port numbers.
  std::string endpoint;
  std::string::size_type length = endpointsStr.size();
  std::string::size_type pos = 0;
  do {
    pos = endpointsStr.find(',', 0);
    if (pos != std::string::npos) {
      endpoint = endpointsStr.substr(0, pos);
      pos += 1;  // skip ','
      length -= (pos);
      endpointsStr = endpointsStr.substr(pos, length);
    } else {
      endpoint = endpointsStr;
    }
    // trim white spaces.
    std::string::size_type wpos = endpoint.find_last_not_of(' ');
    if (wpos != std::string::npos) {
      endpoint.erase(wpos + 1);
      wpos = endpoint.find_first_not_of(' ');
      if (wpos != std::string::npos) endpoint.erase(0, wpos);
      endpointNames.insert(endpoint);
    }
  } while (pos != std::string::npos);
}

char* Utils::copyString(const char* str) {
  char* resStr = NULL;
  if (str != NULL) {
    size_t strSize = strlen(str) + 1;
    resStr = new char[strSize];
    memcpy(resStr, str, strSize);
  }
  return resStr;
}

CacheableStringPtr Utils::convertBytesToString(const uint8_t* bytes,
                                               int32_t length,
                                               size_t maxLength) {
  if (bytes != NULL) {
    std::string str;
    size_t totalBytes = 0;
    char byteStr[20];
    for (int32_t index = 0; index < length; ++index) {
      int len = ACE_OS::snprintf(byteStr, 20, "%d ", bytes[index]);
      totalBytes += len;
      // no use going beyond maxLength since LOG* methods will truncate
      // in any case
      if (maxLength > 0 && totalBytes > maxLength) {
        break;
      }
      str.append(byteStr, len);
    }
    return CacheableString::create(str.data(),
                                   static_cast<int32_t>(str.size()));
  }
  return CacheableString::create("");
}

int32_t Utils::logWideString(char* buf, size_t maxLen, const wchar_t* wStr) {
  if (wStr != NULL) {
    mbstate_t state;
    ACE_OS::memset(&state, 0, sizeof(mbstate_t));
    const char* bufStart = buf;
    do {
      if (maxLen < static_cast<size_t>(MB_CUR_MAX)) {
        break;
      }
      size_t numChars = wcrtomb(buf, *wStr, &state);
      if (numChars == static_cast<size_t>(-1)) {
        // write short when conversion cannot be done
        numChars = ACE_OS::snprintf(buf, maxLen, "<%u>", *wStr);
      }
      buf += numChars;
      if (numChars >= maxLen) {
        break;
      }
      maxLen -= numChars;
    } while (*wStr++ != L'\0');
    return static_cast<int32_t>(buf - bufStart);
  } else {
    return ACE_OS::snprintf(buf, maxLen, "null");
  }
}
