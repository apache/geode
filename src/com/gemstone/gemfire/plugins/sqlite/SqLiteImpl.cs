using System;
using System.Data;
using System.Data.SQLite;
using GemStone.GemFire.Cache.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
namespace GemStone.GemFire.Plugins.SQLite
{

  public class SqLiteImpl<TKey, TValue> : IPersistenceManager<TKey, TValue>
  {

    #region Factory Method
    public static SqLiteImpl<TKey, TValue> Create()
    {
      return new SqLiteImpl<TKey, TValue>();
    }

    #endregion //Factory Method

    #region IPersistenceManager<TKey,TValue> Members


    public void Init(IRegion<TKey, TValue> region, Properties<string, string> diskProperties)
    {
      try
      {
        string pageSize, maxPageCount, persistenceDir;
        m_tableName = region.Name;

        if (diskProperties == null)
        {
          persistenceDir = DefaultPersistenceDir;
          pageSize = DefaultPageSize;
          maxPageCount = DefaultMaxPageCount;
        }
        else
        {
          persistenceDir = string.IsNullOrEmpty(diskProperties.Find(PersistenceDir)) ? DefaultPersistenceDir : diskProperties.Find(PersistenceDir);
          pageSize = string.IsNullOrEmpty(diskProperties.Find(PageSize)) ? DefaultPageSize : diskProperties.Find(PageSize);
          maxPageCount = string.IsNullOrEmpty(diskProperties.Find(MaxPageCount)) ? DefaultMaxPageCount : diskProperties.Find(MaxPageCount);
        }
        Log.Debug("InitalizeSqLite called with MaxPageCount:{0} PageSize:{1} PersistenceDir:{2}", maxPageCount, pageSize, persistenceDir);

        // create region db file
        m_persistenceDir = Path.Combine(Directory.GetCurrentDirectory(), persistenceDir);
        m_regionDir = Path.Combine(m_persistenceDir, m_tableName);
        Directory.CreateDirectory(m_regionDir);

        //create sqlite connection string
        m_connectionString = string.Format("Data Source={0};Version=3;Page Size={1};Max Page Count={2};",
          Path.Combine(m_regionDir, m_tableName + ".db"), pageSize, maxPageCount);
        Log.Debug("Created connection string : {0}", m_connectionString);
        SqliteHelper.InitalizeSqLite(m_tableName, m_connectionString);
      }
      catch (Exception ex)
      {
        Log.Error("Exception in SqLiteImpl.Init: {0}", ex);
        throw;
      }

    }

    public TValue Read(TKey key)
    {
      try
      {
        return (TValue)SqliteHelper.GetValue(key, m_tableName, m_connectionString);
      }
      catch (Exception ex)
      {
        Log.Error("Exceptn in SqLiteImpl.Init: {0}", ex);
        throw;
      }
    }

    public void Write(TKey key, TValue value)
    {
      try
      {
        SqliteHelper.InsertKeyValue(key, value, m_tableName, m_connectionString);
      }
      catch (Exception ex)
      {
        Log.Error("Exceptn in SqLiteImpl.Init: {0}", ex);
        throw;
      }
    }

    public bool WriteAll()
    {
      throw new NotImplementedException();
    }

    public bool ReadAll()
    {
      throw new NotImplementedException();
    }

    public void Destroy(TKey key)
    {
      try
      {
        SqliteHelper.RemoveKey(key, m_tableName, m_connectionString);
      }
      catch (Exception ex)
      {
        Log.Error("Exceptn in SqLiteImpl.Init: {0}", ex);
        throw;
      }
    }

    public void Close()
    {
      try
      {
        SqliteHelper.CloseSqlite();
        Directory.Delete(m_regionDir, true);
        Directory.Delete(m_persistenceDir);
      }
      catch (Exception ex)
      {
        Log.Error("Exceptn in SqLiteImpl.Init: {0}", ex);
      }
    }

    #endregion //IPersistenceManager<TKey,TValue> Members

    #region Data Members
    private string m_connectionString;
    private string m_tableName;
    private string m_persistenceDir;
    private string m_regionDir;
    #endregion

    #region Constants
    public static readonly string PersistenceDir = "PersistenceDirectory";
    public static readonly string MaxPageCount = "max_page_count";
    public static readonly string PageSize = "page_size";
    public static readonly string DefaultPersistenceDir = "GemFireRegionData";
    public static readonly string DefaultMaxPageCount = "2147483646";
    public static readonly string DefaultPageSize = "65536";
    #endregion

  }

  internal static class SqliteHelper
  {
    public static byte[] GetBytes(object obj)
    {
      BinaryFormatter bf = new BinaryFormatter();
      MemoryStream ms = new MemoryStream();
      bf.Serialize(ms, obj);
      return ms.ToArray();
    }

    public static object GetObject(byte[] bytes)
    {
      BinaryFormatter bf = new BinaryFormatter();
      MemoryStream ms = new MemoryStream(bytes);
      return bf.Deserialize(ms);
    }

    public static void InitalizeSqLite(string tableName, string connectionString)
    {

      //construct create table query for this region
      string query = string.Format("CREATE TABLE IF NOT EXISTS {0}(key BLOB PRIMARY KEY,value BLOB);", tableName);

      using (SQLiteConnection conn = new SQLiteConnection(connectionString))
      {
        conn.Open();
        using (SQLiteCommand command = new SQLiteCommand(query, conn))
        {
          Log.Debug("Executing query:{0} ", command.CommandText);
          command.ExecuteNonQuery();
        }
      }
      Log.Debug("Initialization Completed");
    }

    public static void ExecutePragma(string pragmaName, string pragmaValue, string connectionString)
    {
      string pragmaQuery = string.Format("PRAGMA {0} = {1};", pragmaName, pragmaValue);
      using (SQLiteConnection conn = new SQLiteConnection(connectionString))
      {
        conn.Open();
        using (SQLiteCommand command = new SQLiteCommand(pragmaQuery, conn))
        {
          Log.Debug("Executing query:{0} ", command.CommandText);
          command.ExecuteNonQuery();
        }
      }
    }

    public static void InsertKeyValue(object key, object value, string tableName, string connectionString)
    {
      //construct query
      string query = string.Format("REPLACE INTO {0} VALUES(@key,@value);", tableName);
      using (SQLiteConnection conn = new SQLiteConnection(connectionString))
      {
        conn.Open();
        using (SQLiteCommand command = new SQLiteCommand(query, conn))
        {
          command.Parameters.Add(new SQLiteParameter("@key", key));
          command.Parameters.Add(new SQLiteParameter("@value", GetBytes(value)));
          Log.Debug("Executing query:{0} ", command.CommandText);
          command.ExecuteNonQuery();
        }
      }
    }

    public static object GetValue(object key, string tableName, string connectionString)
    {
      //construct query
      string query = string.Format("SELECT value FROM {0} WHERE key=@key;", tableName);
      object retValue;
      using (SQLiteConnection conn = new SQLiteConnection(connectionString))
      {
        conn.Open();
        using (SQLiteCommand command = new SQLiteCommand(query, conn))
        {
          // create parameters and execute
          command.Parameters.Add(new SQLiteParameter("@key", key));
          Log.Debug("Executing query:{0} ", command.CommandText);
          retValue = SqliteHelper.GetObject((byte[])command.ExecuteScalar());
        }
      }
      return retValue;
    }

    public static void RemoveKey(object key, string tableName, string connectionString)
    {
      //construct query
      string query = string.Format("DELETE FROM {0} WHERE key=@key;", tableName);
      using (SQLiteConnection conn = new SQLiteConnection(connectionString))
      {
        conn.Open();
        using (SQLiteCommand command = new SQLiteCommand(query, conn))
        {
          // create parameters and execute
          command.Parameters.Add(new SQLiteParameter("@key", key));
          Log.Debug("Executing query:{0} ", command.CommandText);
          command.ExecuteNonQuery();
        }
      }
    }

    public static void CloseSqlite()
    {
      // remove the region db files
      //Directory.Delete(m_persistenceDir, true);
    }


  }
}
