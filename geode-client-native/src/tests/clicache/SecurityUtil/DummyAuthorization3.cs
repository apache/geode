using System;
using System.Collections.Generic;
using System.Text;

namespace GemStone.GemFire.Cache.Tests
{
  public class DummyAuthorization3 : CredentialGenerator
  {
    public override CredentialGenerator.ClassCode GetClassCode()
    {
      return ClassCode.Dummy;//TODO:hitesh this need to fix properly
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

    public override Properties GetValidCredentials(int index)
    {
      Properties p = Properties.Create();
      p.Insert("security-username", "user" + index);
      p.Insert("security-password", "user" + index);

      return p;
    }

    public override Properties GetValidCredentials(Properties principal)
    {
      Properties p = Properties.Create();
      p.Insert("security-username", "user" + 100);
      p.Insert("security-password", "user" + 100);

      return p;
    }

    public override Properties GetInvalidCredentials(int index)
    {
      Properties p = Properties.Create();
      p.Insert("security-username", "user" + index);
      p.Insert("security-password", "12user" + index);

      return p;
    }

    protected override Properties Init()
    {
      return null;
    }
  }
}
