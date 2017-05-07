//=========================================================================
// Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
// This product is protected by U.S. and international copyright
// and intellectual property laws. Pivotal products are covered by
// more patents listed at http://www.pivotal.io/patents.
//========================================================================

using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Xml;

namespace GemStone.GemFire.DUnitFramework
{
  public class XmlNodeReaderWriter
  {
    #region Public accessors, constants and members

    public XmlDocument XmlDoc
    {
      get
      {
        return m_xmlDoc;
      }
    }
    public const string RootNodeName = "Settings";

    #endregion

    #region Statics and constants

    private static Dictionary<string, XmlNodeReaderWriter> instances = new Dictionary<string, XmlNodeReaderWriter>();

    #endregion

    #region Private members

    private FileStream m_xmlStream = null;
    private XmlReader m_xmlReader = null;
    private volatile XmlDocument m_xmlDoc = null;
    private string m_xmlPath = null;

    #endregion

    private void Close()
    {
      if (m_xmlReader != null)
      {
        m_xmlReader.Close();
        m_xmlStream.Close();
      }
    }

    private void ReloadFile(string path)
    {
      XmlReaderSettings xmlSettings = new XmlReaderSettings();
      xmlSettings.IgnoreComments = true;
      m_xmlStream = new FileStream(path, FileMode.Open, FileAccess.Read,
        FileShare.Read);
      m_xmlReader = XmlReader.Create(m_xmlStream, xmlSettings);
      m_xmlDoc = new XmlDocument();
      m_xmlDoc.Load(m_xmlReader);
      m_xmlPath = path;
    }

    private void Save()
    {
      if (m_xmlDoc != null)
      {
        Close();
        m_xmlDoc.Save(m_xmlPath);
        ReloadFile(m_xmlPath);
      }
    }

    /// <summary>
    /// Private constructor to create a ReadConfig instance for the given
    /// configuration file.
    /// </summary>
    /// <param name="path">The path of the XML file to read.</param>
    private XmlNodeReaderWriter(string path)
    {
      try
      {
        if (!File.Exists(path))
        {
          FileStream fs = new FileStream(path, FileMode.Create,
            FileAccess.Write, FileShare.Read);
          if (fs != null)
          {
            byte[] buffer = Encoding.ASCII.GetBytes(string.Format(
              "<?xml version=\"1.0\" encoding=\"utf-8\"?>{0}<{1}>{2}</{3}>{4}",
              Environment.NewLine, RootNodeName, Environment.NewLine,
              RootNodeName, Environment.NewLine));
            fs.Write(buffer, 0, buffer.Length);
            fs.Close();
          }
          else
          {
            m_xmlDoc = null;
            return;
          }
        }
        ReloadFile(path);
      }
      catch
      {
        Close();
        m_xmlDoc = null;
        throw;
      }
    }

    /// <summary>
    /// Get an instance of ReadConfig for the given configuration file.
    /// </summary>
    /// <param name="path">The path of the XML file to read.</param>
    /// <returns>An instance of ReadConfig object to be able to read key/value pairs.</returns>
    public static XmlNodeReaderWriter GetInstance(string path)
    {
      XmlNodeReaderWriter instance = null;
      if (path != null)
      {
        lock (((ICollection)instances).SyncRoot)
        {
          if (instances.ContainsKey(path))
          {
            instance = instances[path];
          }
          else
          {
            instance = new XmlNodeReaderWriter(path);
            instances.Add(path, instance);
          }
        }
      }
      return instance;
    }

    /// <summary>
    /// Remove an instance from the global table.
    /// </summary>
    /// <param name="path">The path of the XML file whose instance is to be removed.</param>
    public static void RemoveInstance(string path)
    {
      if (path != null)
      {
        lock (((ICollection)instances).SyncRoot)
        {
          if (instances.ContainsKey(path))
          {
            XmlNodeReaderWriter instance = instances[path];
            instance.Close();
            instances.Remove(path);
          }
        }
      }
    }

    public static string GetPathForNode(MethodBase method)
    {
      return '/' + RootNodeName + '/' + method.ReflectedType.Name +
        '/' + method.Name;
    }

    public static string GetPathForNode(MethodBase method, string nodeName)
    {
      return GetPathForNode(method.ReflectedType.Name, method.Name, nodeName);
    }

    public static string GetPathForNode(string typeName)
    {
      return '/' + RootNodeName + '/' + typeName;
    }

    public static string GetPathForNode(string typeName, string methodName, string nodeName)
    {
      return '/' + RootNodeName + '/' + typeName +
        '/' + methodName + '/' + nodeName;
    }

    /// <summary>
    /// Select a single XML node having the given xpath.
    /// </summary>
    /// <param name="xpath">The XPath of the node to search.</param>
    /// <returns>The XML node.</returns>
    public XmlNode GetNode(string xpath)
    {
      if (m_xmlDoc != null && xpath != null)
      {
        lock (m_xmlDoc)
        {
          return m_xmlDoc.SelectSingleNode(xpath);
        }
      }
      return null;
    }

    /// <summary>
    /// Select the XML nodes having the given xpath.
    /// </summary>
    /// <param name="xpath">The XPath of the node to search.</param>
    /// <returns>The list of XML nodes.</returns>
    public XmlNodeList GetNodeList(string xpath)
    {
      if (m_xmlDoc != null && xpath != null)
      {
        lock (m_xmlDoc)
        {
          return m_xmlDoc.SelectNodes(xpath);
        }
      }
      return null;
    }

    /// <summary>
    /// Get the innner text of a node using given xpath.
    /// </summary>
    /// <param name="xpath">The XPath of the node to search.</param>
    /// <returns>The value of the node.</returns>
    public string GetValue(string xpath)
    {
      if (m_xmlDoc != null && xpath != null)
      {
        lock (m_xmlDoc)
        {
          XmlNode node = m_xmlDoc.SelectSingleNode(xpath);
          if (node != null)
          {
            return node.InnerText;
          }
        }
      }
      return null;
    }

    /// <summary>
    /// Get the given attribute of a node having the given xpath.
    /// </summary>
    /// <param name="xpath">The XPath of the node to search.</param>
    /// <param name="attribName">The name of the attribute to find.</param>
    /// <returns>The value of the attribute of the node.</returns>
    public string GetAttribute(string xpath, string attribName)
    {
      if (m_xmlDoc != null && xpath != null)
      {
        lock (m_xmlDoc)
        {
          XmlNode node = m_xmlDoc.SelectSingleNode(xpath + "/@" + attribName);
          if (node != null)
          {
            return node.Value;
          }
        }
      }
      return null;
    }

    /// <summary>
    /// Get an attribute of a node for the given XmlNode.
    /// </summary>
    /// <param name="node">The XmlNode to search in.</param>
    /// <param name="xpath">The XPath of the node to search.</param>
    /// <param name="attribName">The name of the attribute to find.</param>
    /// <returns>The value of the attribute of the node.</returns>
    public static string GetAttribute(XmlNode node, string attribName)
    {
      if (node != null)
      {
        XmlAttribute attrib = node.Attributes[attribName];
        if (attrib != null)
        {
          return attrib.Value;
        }
      }
      return null;
    }

    /// <summary>
    /// Get the attributes for nodes having the given xpath.
    /// </summary>
    /// <param name="xpath">The XPath of the nodes to search.</param>
    /// <returns>
    /// List of attributes as key-value pairs for the found nodes.
    /// </returns>
    public List<Dictionary<string, string>> GetAttributes(string xpath)
    {
      if (m_xmlDoc != null)
      {
        lock (m_xmlDoc)
        {
          return GetAttributes(m_xmlDoc, xpath);
        }
      }
      return null;
    }

    /// <summary>
    /// Get the attributes for nodes having the given xpath for the given XmlNode.
    /// </summary>
    /// <param name="node">The XmlNode to search in.</param>
    /// <param name="xpath">The XPath of the nodes to search.</param>
    /// <returns>
    /// List of attributes as key-value pairs for the found nodes.
    /// </returns>
    public static List<Dictionary<string, string>> GetAttributes(
      XmlNode node, string xpath)
    {
      if (xpath != null)
      {
          XmlNodeList nodeList = node.SelectNodes(xpath);
          if (nodeList != null)
          {
            List<Dictionary<string, string>> valueList = new List<Dictionary<string, string>>();
            foreach (XmlNode xmlNode in nodeList)
            {
              Dictionary<string, string> keyValPairs = new Dictionary<string, string>();
              foreach (XmlAttribute xmlAttrib in xmlNode.Attributes)
              {
                keyValPairs.Add(xmlAttrib.Name, xmlAttrib.Value);
              }
              valueList.Add(keyValPairs);
            }
            return valueList;
        }
      }
      return null;
    }

    #region Convenience functions to get attributes

    public static string GetStringValue(XmlNode node, string attribName)
    {
      return GetStringValue(node, attribName, null);
    }

    public static string GetStringValue(XmlNode node, string attribName,
      string defaultValue)
    {
      XmlAttribute xmlAttrib = node.Attributes[attribName];
      if (xmlAttrib != null && xmlAttrib.Value != null)
      {
        return xmlAttrib.Value;
      }
      return defaultValue;
    }

    public static int String2Int(string str, int defaultValue)
    {
      try
      {
        return int.Parse(str);
      }
      catch
      {
        return defaultValue;
      }
    }

    /// <summary>
    /// Get the number of seconds from the string.
    /// </summary>
    /// <remarks>
    /// If the string contains 'h' or 'm' then it is assumed to be in
    /// hours and minutes respectively.
    /// </remarks>
    /// <param name="str">The string to parse.</param>
    /// <param name="defaultValue">
    /// The default value to return if string is null or empty.
    /// </param>
    /// <returns>
    /// The number of seconds;
    /// <paramref name="defaultValue"/> if string is null of empty.
    /// </returns>
    public static int String2Seconds(string str, int defaultValue)
    {
      if (str == null || str.Length == 0)
      {
        return defaultValue;
      }
      int totalSecs = 0;
      int hPos = str.IndexOf('h');
      if (hPos > 0)
      {
        totalSecs += int.Parse(str.Substring(0, hPos)) * 3600;
        str = str.Substring(hPos + 1);
      }
      int mPos = str.IndexOf('m');
      if (mPos > 0)
      {
        totalSecs += int.Parse(str.Substring(0, mPos)) * 60;
        str = str.Substring(mPos + 1);
      }
      if (str != null && str.Length > 0)
      {
        str = str.TrimEnd('s');
        totalSecs += int.Parse(str);
      }
      return totalSecs;
    }

    public static int GetIntValue(XmlNode node, string attribName,
      int defaultValue)
    {
      XmlAttribute xmlAttrib = node.Attributes[attribName];
      if (xmlAttrib != null)
      {
        return String2Int(xmlAttrib.Value, defaultValue);
      }
      return defaultValue;
    }

    public static bool String2Bool(string str, bool defaultValue)
    {
      str = str.ToLower();
      if (str == "true" || str == "yes")
      {
        return true;
      }
      else if (str == "false" || str == "no")
      {
        return false;
      }
      return defaultValue;
    }

    public static bool GetBoolValue(XmlNode node, string attribName,
      bool defaultValue)
    {
      XmlAttribute xmlAttrib = node.Attributes[attribName];
      if (xmlAttrib != null)
      {
        return String2Bool(xmlAttrib.Value, defaultValue);
      }
      return defaultValue;
    }

    #endregion

    /// <summary>
    /// Get the attributes for a node in the given method.
    /// </summary>
    /// <param name="method">The method to search the node.</param>
    /// <param name="nodeName">The name of the node in the given method.</param>
    /// <returns>
    /// List of attributes as key-value pairs for the found nodes.
    /// </returns>
    public List<Dictionary<string, string>> GetValues(MethodBase method, string nodeName)
    {
      string xpath = GetPathForNode(method, nodeName);
      return GetAttributes(xpath);
    }

    /// <summary>
    /// Get the attributes for a node in the given method.
    /// </summary>
    /// <param name="typeName">The class to which the method belongs.</param>
    /// <param name="methodName">The name of the method.</param>
    /// <param name="nodeName">The name of the node in the given method.</param>
    /// <returns>
    /// List of attributes as key-value pairs for the found nodes.
    /// </returns>
    public List<Dictionary<string, string>> GetValues(string typeName, string methodName, string nodeName)
    {
      string xpath = '/' + RootNodeName + '/' + typeName +
        '/' + methodName + '/' + nodeName;
      return GetAttributes(xpath);
    }

    /// <summary>
    /// Convenience method to add a new node with the given path and attributes.
    /// All the ancestors of the node that do not exist are also added.
    /// Normally should not be used rather XML created manually.
    /// </summary>
    /// <param name="nodePath">
    /// The path of the node to add.
    /// This should be a '/' separated path like in XPath.
    /// </param>
    /// <param name="attributes">
    /// Any number of attributes as key value pairs, with a key followed by a value.
    /// If they are not even in number then the last value is taken to be empty.
    /// </param>
    public void AddNodeAttributes(string nodePath, params string[] attributes)
    {
      if (m_xmlDoc != null && nodePath != null)
      {
        string[] nodePathSplit = nodePath.Split('/');
        string nodePartialPath = string.Empty;
        lock (m_xmlDoc)
        {
          XmlNode currNode = m_xmlDoc;
          XmlNode parentNode = m_xmlDoc;
          XmlNode newNode;
          int i;
          for (i = 0; i < nodePathSplit.Length - 1; i++)
          {
            nodePartialPath += '/' + nodePathSplit[i];
            currNode = m_xmlDoc.SelectSingleNode(nodePartialPath);
            if (currNode == null)
            {
              break;
            }
            parentNode = currNode;
          }
          for (int j = i; j < nodePathSplit.Length; j++)
          {
            newNode = m_xmlDoc.CreateElement(nodePathSplit[j]);
            parentNode.AppendChild(newNode);
            parentNode = newNode;
          }
          XmlAttribute xmlAttrib;
          for (i = 0; i < attributes.Length; i += 2)
          {
            xmlAttrib = m_xmlDoc.CreateAttribute(attributes[i]);
            if ((i + 1) < attributes.Length)
            {
              xmlAttrib.Value = attributes[i + 1];
            }
            else
            {
              xmlAttrib.Value = string.Empty;
            }
            parentNode.Attributes.Append(xmlAttrib);
          }
          Save();
        }
      }
    }

    /// <summary>
    /// Delete the first instance of a node with the given path.
    /// </summary>
    /// <param name="nodePath">
    /// The path of the node to delete.
    /// This should be a '/' separated path like in XPath.
    /// </param>
    public void DeleteNode(string nodePath)
    {
      if (m_xmlDoc != null && nodePath != null)
      {
        lock (m_xmlDoc)
        {
          XmlNode node = m_xmlDoc.SelectSingleNode(nodePath);
          if (node != null)
          {
            node.ParentNode.RemoveChild(node);
            Save();
          }
        }
      }
    }

    /// <summary>
    /// Delete all instances of a node with the given path.
    /// </summary>
    /// <param name="nodePath">
    /// The path of the node to delete.
    /// This should be a '/' separated path like in XPath.
    /// </param>
    public void DeleteAllNodes(string nodePath)
    {
      if (m_xmlDoc != null && nodePath != null)
      {
        lock (m_xmlDoc)
        {
          XmlNodeList nodeList = m_xmlDoc.SelectNodes(nodePath);
          if (nodeList != null)
          {
            foreach (XmlNode node in nodeList)
            {
              node.ParentNode.RemoveChild(node);
            }
            Save();
          }
        }
      }
    }

    /// <summary>
    /// Set the attributes for the first instance of a node with the given path.
    /// If the node is not found then it is added (alongwith all its parents).
    /// </summary>
    /// <param name="nodePath">
    /// The path of the node to modify.
    /// This should be a '/' separated path like in XPath.
    /// </param>
    /// <param name="attributes">
    /// The given key value pairs are set as attributes in the first found node.
    /// If they are not even in number then the last value is taken to be empty.
    /// </param>
    public void SetNodeAttributes(string nodePath, params string[] attributes)
    {
      if (m_xmlDoc != null && nodePath != null)
      {
        lock (m_xmlDoc)
        {
          XmlNode xmlNode = m_xmlDoc.SelectSingleNode(nodePath);
          if (xmlNode != null)
          {
            xmlNode.Attributes.RemoveAll();
            XmlAttribute xmlAttrib;
            for (int i = 0; i < attributes.Length; i += 2)
            {
              xmlAttrib = m_xmlDoc.CreateAttribute(attributes[i]);
              if ((i + 1) < attributes.Length)
              {
                xmlAttrib.Value = attributes[i + 1];
              }
              else
              {
                xmlAttrib.Value = string.Empty;
              }
              xmlNode.Attributes.Append(xmlAttrib);
            }
            Save();
          }
          else
          {
            AddNodeAttributes(nodePath, attributes);
          }
        }
      }
    }

    /// <summary>
    /// Create a XmlNode from a given XML string.
    /// </summary>
    /// <param name="xmlString">The XML string for a node.</param>
    /// <returns>The XMLNode created from the given string.</returns>
    public static XmlNode CreateXmlNode(string xmlString)
    {
      XmlDocument xmlDoc = new XmlDocument();
      xmlDoc.LoadXml(xmlString);
      return xmlDoc.FirstChild;
    }
  }
}
