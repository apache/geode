using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
  using GemStone.GemFire.Cache.Generic;
  public class DummyAuthorization3 : CredentialGenerator
  {
    public override CredentialGenerator.ClassCode GetClassCode()
    {
      return ClassCode.Dummy;//TODO: this need to fix properly
    }

    public override string AuthInit
    {
      get { return null; }
    }

    public override string Authenticator
    {
      get { return "templates.security.DummyAuthenticator.create"; }
    }

    public string AuthenticatorPP
    {
      get { return "javaobject.DummyAuthorization3.create"; }
    }

    public override Properties<string, string> GetValidCredentials(int index)
    {
      Properties<string, string> p = Properties<string, string>.Create<string, string>();
      p.Insert("security-username", "user" + index);
      p.Insert("security-password", "user" + index);

      return p;
    }

    public override Properties<string, string> GetValidCredentials(Properties<string, string> principal)
    {
      Properties<string, string> p = Properties<string, string>.Create<string, string>();
      p.Insert("security-username", "user" + 100);
      p.Insert("security-password", "user" + 100);

      return p;
    }

    public override Properties<string, string> GetInvalidCredentials(int index)
    {
      Properties<string, string> p = Properties<string, string>.Create<string, string>();
      p.Insert("security-username", "user" + index);
      p.Insert("security-password", "12user" + index);

      return p;
    }

    protected override Properties<string, string> Init()
    {
      return null;
    }
  }
}
