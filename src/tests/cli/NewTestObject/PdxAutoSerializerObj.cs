using System;

namespace GemStone.GemFire.Cache.Tests.NewAPI
{
   using GemStone.GemFire.Cache.Generic;
    using System.Collections;
    [Serializable]
   public class SerializePdx1
   {
     [PdxIdentityField]
     public int i1;
     public int i2;
     public string s1;
     public string s2;

     /*public static SerializePdx1 CreateDeserializable()
     {
       return new SerializePdx1(false);
     }*/

     public SerializePdx1()
     {
     }
     public SerializePdx1(bool init)
     {
       if (init)
       {
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx1 other = obj as SerializePdx1;

       if (other == null)
         return false;

       if (i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
         return true;

       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }
    [Serializable]
   public class SerializePdx2
   {
     public string s0;
     [PdxIdentityField]
     public int i1;
     public int i2;
     public string s1;
     public string s2;

     public SerializePdx2()
     {

     }
     public override string ToString()
     {
       return i1 + i2 + s1 + s2;
     }
     public SerializePdx2(bool init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx2 other = obj as SerializePdx2;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
         return true;

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }
    [Serializable]
   public class BaseClass
   {
     //private readonly int _b1 = 1000;
     [NonSerialized]
     //private int _nonserialized = 1001;
     //private static int _static = 1002;

     private const int _const = 1003;

     private int _baseclassmember;

     public BaseClass()
     {

     }
     public BaseClass(bool init)
     {
       if (init)
       {
         _baseclassmember = 101;
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       BaseClass bc = obj as BaseClass;
       if (bc == null)
         return false;

       if (bc == this)
         return true;

       if (bc._baseclassmember == this._baseclassmember)
       {
         return true;
       }
       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }

     public void ToData(IPdxWriter w)
     {
       w.WriteInt("_baseclassmember", _baseclassmember);
     }

     public void FromData(IPdxReader r)
     {
       _baseclassmember = r.ReadInt("_baseclassmember");
     }
   }

   public class Address
   {
     private static Guid oddGuid = new Guid("924243B5-9C2A-46S7-86B1-E0B905C7EED3");
     private static Guid evenGuid = new Guid("47AA8F17-FF6B-4a9s-B398-D83790977574");
     private string _street;
     private string _aptName;
     private int _flatNumber;
     private Guid _guid;
     public Address()
     { }
     public Address(int id)
     {
       _flatNumber = id;
       _aptName = id.ToString();
       _street = id.ToString() + "_street";
       if (id % 2 == 0)
         _guid = evenGuid;
       else
         _guid = oddGuid;
     }
     public override string ToString()
     {
       return _flatNumber + " " + _aptName + " " + _street + "  " + _guid.ToString();
     }
     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       Address other = obj as Address;

       if (other == null)
         return false;

       if (_street == other._street &&
           _aptName == other._aptName &&
           _flatNumber == other._flatNumber &&
           _guid.Equals(other._guid))
         return true;

       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }

     public void ToData(IPdxWriter w)
     {
       w.WriteString("_street", _street);
       w.WriteString("_aptName", _aptName);
       w.WriteInt("_flatNumber", _flatNumber);
       w.WriteString("_guid", _guid.ToString());
     }

     public void FromData(IPdxReader r)
     {
       _street = r.ReadString("_street");
       _aptName = r.ReadString("_aptName");
       _flatNumber = r.ReadInt("_flatNumber");
       string s = r.ReadString("_guid");
       _guid = new Guid(s);
     }
   }

   public class SerializePdx3 : BaseClass
   {
     private string s0;
     [PdxIdentityField]
     private int i1;
     public int i2;
     public string s1;
     public string s2;
     private SerializePdx2 nestedObject;
     private ArrayList _addressList;
     private Address _address;
     private Hashtable _hashTable;
     //private Address[] _arrayOfAddress;

     public SerializePdx3()
       : base()
     {

     }

     public SerializePdx3(bool init, int nAddress)
       : base(init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
         nestedObject = new SerializePdx2(true);

         _addressList = new ArrayList();
         _hashTable = new Hashtable();

         for (int i = 0; i < 10; i++)
         {
           _addressList.Add(new Address(i));
           _hashTable.Add(i, new SerializePdx2(true));
         }

         _address = new Address(nAddress);

         //_arrayOfAddress = new Address[3];

         //for (int i = 0; i < 3; i++)
         //{
         //  _arrayOfAddress[i] = new Address(i);
         //}
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx3 other = obj as SerializePdx3;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
       {
         bool ret = nestedObject.Equals(other.nestedObject);
         if (ret)
         {
           if (_addressList.Count == 10 &&
             _addressList.Count == other._addressList.Count//&&
             //_arrayOfAddress.Length == other._arrayOfAddress.Length &&
             //_arrayOfAddress[0].Equals(other._arrayOfAddress[0])
             )
           {
             for (int i = 0; i < _addressList.Count; i++)
             {
               ret = _addressList[i].Equals(other._addressList[i]);
               if (!ret)
                 return false;
             }

             if (_hashTable.Count != other._hashTable.Count)
               return false;
             foreach (DictionaryEntry de in _hashTable)
             {
               object otherHe = other._hashTable[de.Key];
               ret = de.Value.Equals(otherHe);
               if (!ret)
                 return false;
             }

             if (!_address.Equals(other._address))
               return false;
             return base.Equals(other);
           }
         }
       }

       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }

     public new void ToData(IPdxWriter w)
     {
       base.ToData(w);
       w.WriteString("s0", s0);
       w.WriteInt("i1", i1);
       w.WriteInt("i2", i2);
       w.WriteString("s1", s1);
       w.WriteString("s2", s2);
       w.WriteObject("nestedObject", nestedObject);
       w.WriteObject("_addressList", _addressList);
       w.WriteObject("_address", _address);
       w.WriteObject("_hashTable", _hashTable);
     }

     public new void FromData(IPdxReader r)
     {
       base.FromData(r);
       s0 = r.ReadString("s0");
       i1 = r.ReadInt("i1");
       i2 = r.ReadInt("i2");
       s1 = r.ReadString("s1");
       s2 = r.ReadString("s2");
       nestedObject = (SerializePdx2)r.ReadObject("nestedObject");
       _addressList = (ArrayList)r.ReadObject("_addressList");
       _address = (Address)r.ReadObject("_address");
       _hashTable = (Hashtable)r.ReadObject("_hashTable");
     }
   }

   public class SerializePdx4 : BaseClass
   {
     private string s0;
     [PdxIdentityField]
     private int i1;
     public int i2;
     public string s1;
     public string s2;
     private SerializePdx2 nestedObject;
     private ArrayList _addressList;

     public SerializePdx4()
       : base()
     {

     }
     public override string ToString()
     {
       return i1 + ":" + i2 + ":" + s1 + ":" + s2 + nestedObject.ToString() + " add: " + _addressList[0].ToString();
     }
     public SerializePdx4(bool init)
       : base(init)
     {
       if (init)
       {
         s0 = "s9999999999999999999999999999999999";
         i1 = 1;
         i2 = 2;
         s1 = "s1";
         s2 = "s2";
         nestedObject = new SerializePdx2(true);

         _addressList = new ArrayList();

         for (int i = 0; i < 10; i++)
         {
           _addressList.Add(new Address(i));
         }
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;
       if (obj == this)
         return true;

       SerializePdx4 other = obj as SerializePdx4;

       if (other == null)
         return false;

       if (s0 == other.s0
          && i1 == other.i1
          && i2 == other.i2
           && s1 == other.s1
            && s2 == other.s2)
       {
         bool ret = nestedObject.Equals(other.nestedObject);
         if (ret)
         {
           if (_addressList.Count == other._addressList.Count &&
             _addressList[0].Equals(other._addressList[0]))
           {
             for (int i = 0; i < _addressList.Count; i++)
             {
               ret = _addressList[i].Equals(other._addressList[i]);
               if (!ret)
                 return false;
             }
             return base.Equals(other);
           }
         }
       }

       return false;
     }
     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }

   public class PdxFieldTest
   {
     string _notInclude = "default_value";
     int _nameChange;
     int _identityField;

     public PdxFieldTest()
     { 
     
     }

     public string NotInclude
     {
       set { _notInclude = "default_value"; }
     }

     public PdxFieldTest(bool init)
     {
       if (init)
       {
         _notInclude = "valuechange";
         _nameChange = 11213;
         _identityField = 1038193;
       }
     }

     public override bool Equals(object obj)
     {
       if (obj == null)
         return false;

       PdxFieldTest other = obj as PdxFieldTest;

       if (other == null)
         return false;

       if (_notInclude == other._notInclude
           && _nameChange == other._nameChange
              && _identityField == other._identityField)
         return true;


       return false;
     }

     public override int GetHashCode()
     {
         return base.GetHashCode();
     }
   }
}
