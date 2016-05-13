//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System.Collections.Generic;
using System.IO;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.DUnitFramework;
  using GemStone.GemFire.Templates.Cache.Security;
  using GemStone.GemFire.Cache.Generic;

  public class XmlAuthzCredentialGenerator : AuthzCredentialGenerator
  {
    enum Role
    {
      Reader,
      Writer,
      Query,
      Admin
    }

    private const string DocURIProp = "security-authz-xml-uri";

    private const string DummyXml = "authz-dummy.xml";

    private const string LdapXml = "authz-ldap.xml";

    private const string PkcsXml = "authz-pkcs.xml";

    private static string[] QueryRegions = { "Portfolios", "Positions" };

    public static OperationCode[] ReaderOps = { OperationCode.Get, OperationCode.GetAll,
      OperationCode.GetServerKeys,
      OperationCode.RegisterInterest, OperationCode.UnregisterInterest, OperationCode.ExecuteCQ };

    public static OperationCode[] WriterOps = { OperationCode.Put, OperationCode.PutAll, OperationCode.RemoveAll,
      OperationCode.Destroy, OperationCode.ExecuteFunction };

    public static OperationCode[] QueryOps = { OperationCode.Query };

    private static Dictionary<OperationCode, bool> readerOpsSet;

    private static Dictionary<OperationCode, bool> writerOpsSet;

    private static Dictionary<OperationCode, bool> queryOpsSet;

    private static Dictionary<string, bool> queryRegionSet;

    static XmlAuthzCredentialGenerator()
    {
      readerOpsSet = new Dictionary<OperationCode, bool>();
      for (int index = 0; index < ReaderOps.Length; index++)
      {
        readerOpsSet.Add(ReaderOps[index], true);
      }
      writerOpsSet = new Dictionary<OperationCode, bool>();
      for (int index = 0; index < WriterOps.Length; index++)
      {
        writerOpsSet.Add(WriterOps[index], true);
      }
      queryOpsSet = new Dictionary<OperationCode, bool>();
      for (int index = 0; index < QueryOps.Length; index++)
      {
        queryOpsSet.Add(QueryOps[index], true);
      }
      queryRegionSet = new Dictionary<string, bool>();
      for (int index = 0; index < QueryRegions.Length; index++)
      {
        queryRegionSet.Add(QueryRegions[index], true);
      }
    }

    public XmlAuthzCredentialGenerator()
    {
    }

    protected override Properties<string, string> Init()
    {
      Properties<string, string> sysProps = new Properties<string, string>();
      string dirName = m_cGen.ServerDataDir;
      if (dirName != null && dirName.Length > 0)
      {
        dirName += "/";
      }
      switch (this.m_cGen.GetClassCode())
      {
        case CredentialGenerator.ClassCode.Dummy:
          sysProps.Insert(DocURIProp, dirName + DummyXml);
          break;

        case CredentialGenerator.ClassCode.LDAP:
          sysProps.Insert(DocURIProp, dirName + LdapXml);
          break;

        case CredentialGenerator.ClassCode.PKCS:
          sysProps.Insert(DocURIProp, dirName + PkcsXml);
          break;

        default:
          throw new IllegalArgumentException(
            "No XML defined for XmlAuthorization module to work with " +
            this.m_cGen.Authenticator);
      }
      return sysProps;
    }

    public override ClassCode GetClassCode()
    {
      return ClassCode.XML;
    }

    public override string AccessControl
    {
      get
      {
        return "templates.security.XmlAuthorization.create";
      }
    }

    protected override Properties<string, string> GetAllowedPrincipal(
      OperationCode[] opCodes, string[] regionNames, int index)
    {
      CredentialGenerator.ClassCode cGenCode = this.m_cGen.GetClassCode();
      Role roleType = GetRequiredRole(opCodes, regionNames);
      switch (cGenCode)
      {
        case CredentialGenerator.ClassCode.Dummy:
          return GetDummyPrincipal(roleType, index);
        case CredentialGenerator.ClassCode.LDAP:
          return GetLdapPrincipal(roleType, index);
        case CredentialGenerator.ClassCode.PKCS:
          return GetPKCSPrincipal(roleType, index);
      }
      return null;
    }

    protected override Properties<string, string> GetDisallowedPrincipal(
      OperationCode[] opCodes, string[] regionNames, int index)
    {
      Role roleType = GetRequiredRole(opCodes, regionNames);
      Role disallowedRoleType = Role.Reader;
      switch (roleType)
      {
        case Role.Reader:
          disallowedRoleType = Role.Writer;
          break;
        case Role.Writer:
          disallowedRoleType = Role.Reader;
          break;
        case Role.Query:
          disallowedRoleType = Role.Reader;
          break;
        case Role.Admin:
          disallowedRoleType = Role.Reader;
          break;
      }
      CredentialGenerator.ClassCode cGenCode = this.m_cGen.GetClassCode();
      switch (cGenCode)
      {
        case CredentialGenerator.ClassCode.Dummy:
          return GetDummyPrincipal(disallowedRoleType, index);
        case CredentialGenerator.ClassCode.LDAP:
          return GetLdapPrincipal(disallowedRoleType, index);
        case CredentialGenerator.ClassCode.PKCS:
          return GetPKCSPrincipal(disallowedRoleType, index);
      }
      return null;
    }

    protected override int GetNumPrincipalTries(
      OperationCode[] opCodes, string[] regionNames)
    {
      return 5;
    }

    private Properties<string, string> GetDummyPrincipal(Role roleType, int index)
    {
      string[] admins = new string[] { "root", "admin", "administrator" };
      int numReaders = 3;
      int numWriters = 3;

      switch (roleType)
      {
        case Role.Reader:
          return GetUserPrincipal("reader" + (index % numReaders));
        case Role.Writer:
          return GetUserPrincipal("writer" + (index % numWriters));
        case Role.Query:
          return GetUserPrincipal("reader" + ((index % 2) + 3));
        default:
          return GetUserPrincipal(admins[index % admins.Length]);
      }
    }

    private Properties<string, string> GetLdapPrincipal(Role roleType, int index)
    {
      return GetUserPrincipal(GetLdapUser(roleType, index));
    }

    private Properties<string, string> GetPKCSPrincipal(Role roleType, int index)
    {
      string userName = GetLdapUser(roleType, index);
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(PKCSCredentialGenerator.KeyStoreAliasProp, userName);
      return props;
    }

    private string GetLdapUser(Role roleType, int index)
    {
      const string userPrefix = "gemfire";
      int[] readerIndices = { 3, 4, 5 };
      int[] writerIndices = { 6, 7, 8 };
      int[] queryIndices = { 9, 10 };
      int[] adminIndices = { 1, 2 };

      switch (roleType)
      {
        case Role.Reader:
          int readerIndex = readerIndices[index % readerIndices.Length];
          return (userPrefix + readerIndex);
        case Role.Writer:
          int writerIndex = writerIndices[index % writerIndices.Length];
          return (userPrefix + writerIndex);
        case Role.Query:
          int queryIndex = queryIndices[index % queryIndices.Length];
          return (userPrefix + queryIndex);
        default:
          int adminIndex = adminIndices[index % adminIndices.Length];
          return (userPrefix + adminIndex);
      }
    }

    private Role GetRequiredRole(OperationCode[] opCodes,
      string[] regionNames)
    {
      Role roleType = Role.Admin;
      bool requiresReader = true;
      bool requiresWriter = true;
      bool requiresQuery = true;

      for (int opNum = 0; opNum < opCodes.Length; opNum++)
      {
        if (requiresReader && !readerOpsSet.ContainsKey(opCodes[opNum]))
        {
          requiresReader = false;
        }
        if (requiresWriter && !writerOpsSet.ContainsKey(opCodes[opNum]))
        {
          requiresWriter = false;
        }
        if (requiresQuery && !queryOpsSet.ContainsKey(opCodes[opNum]))
        {
          requiresQuery = false;
        }
      }
      if (requiresReader)
      {
        roleType = Role.Reader;
      }
      else if (requiresWriter)
      {
        roleType = Role.Writer;
      }
      else if (requiresQuery)
      {
        if (regionNames != null && regionNames.Length > 0)
        {
          bool queryUsers = true;
          for (int index = 0; index < regionNames.Length; index++)
          {
            if (queryUsers && !queryRegionSet.ContainsKey(regionNames[index]))
            {
              queryUsers = false;
            }
          }
          if (queryUsers)
          {
            roleType = Role.Query;
          }
        }
      }
      return roleType;
    }

    private Properties<string, string> GetUserPrincipal(string userName)
    {
      Properties<string, string> props = new Properties<string, string>();
      props.Insert(UserPasswordAuthInit.UserNameProp, userName);
      return props;
    }

  }
}
