//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Diagnostics;
using System.EnterpriseServices.Internal;
using System.IO;

using Microsoft.Win32;

namespace GemStone.GemFire
{
  class GacInstall
  {
    #region Private constants

    private const string DOTNETAssemblyFoldersKey =
      @"SOFTWARE\Microsoft\.NETFramework\AssemblyFolders";
    private const string GemFireProductKey = "GemStone.GemFire";

    #endregion

    static int Main(string[] args)
    {
      bool doInstall;
      string filePath;
      string finalDirPath;
      string action;
      int dirArgIndx;

      if (args == null || args.Length == 0)
      {
        ShowUsage();
        return 1;
      }

      if (args[0] == "-u" || args[0] == "/u")
      {
        doInstall = false;
        filePath = args[1];
        action = "Uninstallation";
        dirArgIndx = 2;
      }
      else
      {
        doInstall = true;
        filePath = args[0];
        action = "Installation";
        dirArgIndx = 1;
      }

      try
      {
        if (args.Length == dirArgIndx)
        {
          finalDirPath = Path.GetDirectoryName(filePath);
          if (finalDirPath == null || finalDirPath.Length == 0)
          {
            finalDirPath = ".";
          }
        }
        else if (args.Length == (dirArgIndx + 1))
        {
          finalDirPath = args[dirArgIndx];
        }
        else
        {
          ShowUsage();
          return 1;
        }

        Publish publish = new Publish();
        if (!File.Exists(filePath))
        {
          throw new FileNotFoundException("Given file " +
            filePath + " does not exist.");
        }
        if (!Directory.Exists(finalDirPath))
        {
          throw new DirectoryNotFoundException("Given directory " +
            finalDirPath + " does not exist.");
        }
        filePath = Path.GetFullPath(filePath);
        finalDirPath = Path.GetFullPath(finalDirPath);
        RegistryKey dotNETAssemblyFoldersKey =
          Registry.LocalMachine.OpenSubKey(DOTNETAssemblyFoldersKey, true);
        if (dotNETAssemblyFoldersKey != null)
        {
          dotNETAssemblyFoldersKey.DeleteSubKey(GemFireProductKey, false);
        }
        if (doInstall)
        {
          publish.GacInstall(filePath);
          if (dotNETAssemblyFoldersKey != null)
          {
            RegistryKey productKey =
              dotNETAssemblyFoldersKey.CreateSubKey(GemFireProductKey);
            if (productKey != null)
            {
              productKey.SetValue(null, finalDirPath, RegistryValueKind.String);
            }
          }
        }
        else
        {
          publish.GacRemove(filePath);
        }
        Console.WriteLine("{0} completed successfully.", action);
      }
      catch (Exception ex)
      {
        Console.WriteLine("Error while performing {0}: {1}{2}\t{3}", action,
          ex.GetType(), Environment.NewLine, ex.Message);
        return 1;
      }
      return 0;
    }

    static void ShowUsage()
    {
      string procName = Process.GetCurrentProcess().ProcessName;
      Console.WriteLine("Installs or unstalls an assembly to GAC.");
      Console.WriteLine("Also creates registry entry for Visual Studio.");
      Console.WriteLine();
      Console.WriteLine("Usage: " + procName + " [-u | /u ] <assembly path> [<installation directory>]");
      Console.WriteLine("-u, /u \t\t\t\t Uninstall the given assembly from GAC");
      Console.WriteLine("<assembly path> \t\t Path of the assembly to be installed/uninstalled");
      Console.WriteLine("<installation directory> \t Full directory path of the final location of the assembly.");
      Console.WriteLine("\t\t\t\t This is an optional argument for the case when the final");
      Console.WriteLine("\t\t\t\t location of the assembly is different from the assembly path");
      Console.WriteLine("\t\t\t\t being provided. This is used to make registry entries so that");
      Console.WriteLine("\t\t\t\t the assembly appears in the \"Add Reference\" menu of Visual Studio.");
    }
  }
}
