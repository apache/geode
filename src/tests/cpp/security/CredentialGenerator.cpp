/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

#include "CredentialGenerator.hpp"
#include "DummyCredentialGenerator.hpp"
#include "DummyCredentialGenerator2.hpp"
#include "DummyCredentialGenerator3.hpp"
#include "LdapUserCredentialGenerator.hpp"
#include "PkcsCredentialGenerator.hpp"
#include "NoopCredentialGenerator.hpp"

using namespace gemfire::testframework::security;

CredentialGenerator::registeredClassMap* CredentialGenerator::generatormap =
    NULL;

CredentialGeneratorPtr CredentialGenerator::create(std::string scheme) {
  if (generators().find(scheme) != generators().end()) {
    return generators()[scheme];

    // first call to create, nothing will be registered until now.
  } else if (generators().size() == 0) {
    registerScheme(CredentialGeneratorPtr(new CredentialGenerator()));
    registerScheme(CredentialGeneratorPtr(new DummyCredentialGenerator()));
    registerScheme(CredentialGeneratorPtr(new DummyCredentialGenerator2()));
    registerScheme(CredentialGeneratorPtr(new DummyCredentialGenerator3()));
    registerScheme(CredentialGeneratorPtr(new LdapUserCredentialGenerator()));
    registerScheme(CredentialGeneratorPtr(new PKCSCredentialGenerator()));
    registerScheme(CredentialGeneratorPtr(new NoopCredentialGenerator()));
    return create(scheme);

  } else {
    return CredentialGeneratorPtr(new CredentialGenerator());
  }
}
