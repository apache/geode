//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.Xml;

namespace GemStone.GemFire.Cache.FwkLib
{
  using GemStone.GemFire.DUnitFramework;

  public class FwkClient
  {
    #region Public members

    public static Dictionary<string, ClientBase> GlobalClients =
      new Dictionary<string, ClientBase>();
    public static Dictionary<string, string> GlobalHostGroups =
      new Dictionary<string, string>();

    #endregion

    #region Private constants

    private const string ClientSetNodeName = "client-set";
    private const string ClientNodeName = "client";
    private const string NameAttrib = "name";
    private const string ExcludeAttrib = "exclude";
    private const string BeginAttrib = "begin";
    private const string CountAttrib = "count";
    private const string HostGroupAttrib = "hostGroup";

    #endregion

    public static List<string> ReadClientNames(XmlNode node, bool global)
    {
      List<string> clientNames = new List<string>();
      Dictionary<string, bool> excludeList = new Dictionary<string, bool>();

      if (node != null)
      {
        XmlNodeList clientSetNodes = node.SelectNodes(ClientSetNodeName);
        XmlAttributeCollection xmlAttribs;
        if (clientSetNodes != null)
        {
          foreach (XmlNode clientSetNode in clientSetNodes)
          {
            List<string> clientSetNames = new List<string>();
            bool exclude = false;
            string clientSetName = null;
            string hostGroupName = string.Empty;
            int clientCount = 1;
            int beginIndx = 1;

            xmlAttribs = clientSetNode.Attributes;
            if (xmlAttribs != null)
            {
              foreach (XmlAttribute xmlAttrib in xmlAttribs)
              {
                switch (xmlAttrib.Name)
                {
                  case NameAttrib:
                    clientSetName = xmlAttrib.Value;
                    break;
                  case ExcludeAttrib:
                    exclude = XmlNodeReaderWriter.String2Bool(xmlAttrib.Value, false);
                    break;
                  case CountAttrib:
                    clientCount = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, 1);
                    break;
                  case BeginAttrib:
                    beginIndx = XmlNodeReaderWriter.String2Int(xmlAttrib.Value, 1);
                    break;
                  case HostGroupAttrib:
                    hostGroupName = xmlAttrib.Value;
                    break;
                  default:
                    throw new IllegalArgException("Unknown attribute '" +
                      xmlAttrib.Name + "' found in '" + ClientSetNodeName +
                      "' node.");
                }
              }
            }
            XmlNodeList clientNodeList = clientSetNode.SelectNodes(ClientNodeName);
            if (clientNodeList != null && clientNodeList.Count > 0)
            {
              foreach (XmlNode clientNode in clientNodeList)
              {
                string clientName = string.Empty;
                xmlAttribs = clientNode.Attributes;
                if (xmlAttribs != null)
                {
                  foreach (XmlAttribute xmlAttrib in xmlAttribs)
                  {
                    if (xmlAttrib.Name == NameAttrib)
                    {
                      clientName = xmlAttrib.Value;
                    }
                    else
                    {
                      throw new IllegalArgException("Unknown attribute '" +
                        xmlAttrib.Name + "' found in '" + ClientNodeName +
                        "' node.");
                    }
                  }
                }
                if (exclude)
                {
                  excludeList[clientSetName + '.' + clientName] = true;
                }
                else
                {
                  clientSetNames.Add(clientSetName + '.' + clientName);
                }
              }
            }
            else
            {
              for (int i = beginIndx; i < beginIndx + clientCount; i++)
              {
                if (exclude)
                {
                  excludeList[clientSetName + '.' + i.ToString()] = true;
                }
                else
                {
                  clientSetNames.Add(clientSetName + '.' + i.ToString());
                }
              }
            }
            if (global)
            {
              foreach (string name in clientSetNames)
              {
                GlobalHostGroups.Add(name, hostGroupName);
              }
            }
            clientNames.AddRange(clientSetNames);
          }
        }
        if (clientNames.Count == 0)
        {
          lock (((ICollection)GlobalClients).SyncRoot)
          {
            foreach (string clientName in GlobalClients.Keys)
            {
              if (!excludeList.ContainsKey(clientName))
              {
                clientNames.Add(clientName);
              }
            }
          }
        }
        else if (excludeList.Count > 0)
        {
          int i = 0;
          foreach (string clientName in clientNames)
          {
            if (excludeList.ContainsKey(clientName))
            {
              clientNames.RemoveAt(i);
            }
            i++;
          }
        }
      }
      return clientNames;
    }

    public static List<ClientBase> GetClients(List<string> clientNames)
    {
      List<ClientBase> clients = new List<ClientBase>();
      if (clientNames != null)
      {
        lock (((ICollection)GlobalClients).SyncRoot)
        {
          foreach (string clientName in clientNames)
          {
            if (GlobalClients.ContainsKey(clientName))
            {
              clients.Add(GlobalClients[clientName]);
            }
          }
        }
      }
      return clients;
    }

    public static void NewClient(string clientId, string newClientId)
    {
      lock (((ICollection)GlobalClients).SyncRoot)
      {
        ClientBase client;
        if (GlobalClients.TryGetValue(clientId, out client))
        {
          ClientBase newClient = client.CreateNew(newClientId);
          client.Dispose();
          Util.Log("Created a new client {0} on host {1}.",
            newClient.ID, newClient.HostName);
          GlobalClients[newClientId] = newClient;
        }
      }
    }
  }

  public abstract class ClientTask
  {
    #region Private members

    private bool m_running;
    private DateTime m_startTime;
    private DateTime m_endTime;
    protected int m_iters = 0;

    #endregion

    #region Public accessors

    public bool Running
    {
      get
      {
        return m_running;
      }
    }

    #endregion

    #region Public accessors

    public TimeSpan ElapsedTime
    {
      get
      {
        if (m_running)
        {
          return DateTime.Now - m_startTime;
        }
        else
        {
          return m_endTime - m_startTime;
        }
      }
    }

    public int Iterations
    {
      get
      {
        return m_iters;
      }
    }

    #endregion

    #region Public methods

    public void StartRun()
    {
      m_running = true;
      m_startTime = DateTime.Now;
      m_iters = 0;
    }

    public void EndRun()
    {
      m_running = false;
      m_endTime = DateTime.Now;
    }

    #endregion

    public abstract void DoTask(int iters, object data);
  }
}
