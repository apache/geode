using System;
using System.Diagnostics;
using System.Threading;

namespace GemStone.GemFire.Cache.UnitTests
{
  using NUnit.Framework;
  using GemStone.GemFire.DUnitFramework;

  public class RegionOperation
  {
    protected Region m_region;

    public Region Region
    {
      get
      {
        return m_region;
      }
    }

    public RegionOperation(string RegionName)
    {
      m_region = CacheHelper.GetRegion(RegionName);
    }

    public void PutOp(int key, IGFSerializable CallbackArg)
    { 

      IGFSerializable value = new CacheableString("value");
      for (int i = 1; i <= key; i++)
      {
        Util.Log("PutOp:key={0},value={1}",i,value);
        m_region.Put(i,value,CallbackArg);
      }
    }

    public void InvalidateOp(int key, IGFSerializable CallbackArg)
    {
      for (int i = 1; i <= key; i++)
      {
        Util.Log("InvalidateOp:key={0}", i);
        m_region.LocalInvalidate(i, CallbackArg);
      }
    }

    public void DestroyOp(int key, IGFSerializable CallbackArg)
    {
      for (int i = 1; i <= key; i++)
      {
        Util.Log("DestroyOp:key={0}", i);
        m_region.Destroy(i, CallbackArg);
      }
    }

    public void RemoveOp(int key, IGFSerializable CallbackArg)
    {

      IGFSerializable value = new CacheableString("value");
      for (int i = 1; i <= key; i++) {
        Util.Log("PutOp:key={0},value={1}", i, value);
        m_region.Remove(i, value, CallbackArg);
      }
    }
  }
}