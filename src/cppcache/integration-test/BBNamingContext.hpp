/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
// This these classes should have been in the framework libary. If we ever use
// these, instead of the ACE context, onto windows and Linux then we should
// move them there.
// This will avoid pulling in a lot of framework headers to cause compilation
// grieve, especially with the stl stuff.
#ifndef __BB_NAMING_CONTEXT__
#define __BB_NAMING_CONTEXT__
#include <stdlib.h>
#include <string>
class BBNamingContextClientImpl;
class BBNamingContextClient {
  BBNamingContextClientImpl* m_impl;

 public:
  BBNamingContextClient();
  ~BBNamingContextClient();
  void open();
  void close();
  int rebind(const char* key, const char* value, char* type = NULL);
  void dump();
  int resolve(const char* key, char* value, char* type = NULL);
};
class BBNamingContextServerImpl;
class BBNamingContextServer {
 public:
  BBNamingContextServerImpl* m_impl;
  BBNamingContextServer();
  ~BBNamingContextServer();
};
#endif
