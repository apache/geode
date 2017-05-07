#include "PositionPdx.hpp"
#include <cwchar>
#include <wchar.h>
#include <malloc.h>

using namespace gemfire;
using namespace testobject;

int32_t PositionPdx::cnt = 0;

PositionPdx::PositionPdx()
{
  init();
}

PositionPdx::PositionPdx(const char* id, int32_t out) {
  init();

  size_t strSize = strlen(id) + 1;
  secId = new char[strSize];
  memcpy(secId, id, strSize);

  qty = out * (cnt%2 == 0 ? 10.0 : 100.0);
  mktValue = qty * 1.2345998;
  sharesOutstanding = out;
  //secType = ( wchar_t * )malloc( ( wcslen( L"a" ) + 1 ) * sizeof( wchar_t ) );
  secType =  new char[ ( strlen( "a" ) + 1 ) ];
  strcpy( secType, "a" );

  pid = cnt++;
}

// This constructor is just for some internal data validation test
PositionPdx::PositionPdx(int32_t iForExactVal) {
  init();

  char * id = new char[iForExactVal+2];
  for(int i=0;i<=iForExactVal; i++)
  {
    id[i] = 'a';
  }
  id[iForExactVal+1] = '\0';
  size_t strSize = strlen(id) + 1;
  secId = new char[strSize];
  memcpy(secId, id, strSize);

  delete [] id;
  qty = (iForExactVal%2==0?1000:100);
  mktValue = qty * 2;
  sharesOutstanding = iForExactVal;
  //secType = ( wchar_t * )malloc( ( wcslen( L"a" ) + 1 ) * sizeof( wchar_t ) );
  secType =  new char [ ( strlen( "a" ) + 1 ) ];
  strcpy( secType, "a" );
  pid = iForExactVal;
}

PositionPdx::~PositionPdx() {
  if (secType != NULL) {
    //free(secType);
    delete [] secType;
    secType = NULL;
  }

  if (secId != NULL) {
    //free(secId);
    delete [] secId;
    secId = NULL;
  }
}

void PositionPdx::init()
{

  avg20DaysVol = 0;
  bondRating = NULL;
  convRatio = 0.0;
  country = NULL;
  delta = 0.0;
  industry = 0;
  issuer = 0;
  mktValue = 0.0;
  qty=0.0;
  secId = NULL;
  secLinks = NULL;
  secType = NULL;
  sharesOutstanding = 0;
  underlyer = NULL;
  volatility=0;
  pid=0;
}

void PositionPdx::toData( PdxWriterPtr pw)  {
  pw->writeLong("avg20DaysVol", avg20DaysVol);
  pw->markIdentityField("avg20DaysVol");

  pw->writeString("bondRating", bondRating);
  pw->markIdentityField("bondRating");

  pw->writeDouble("convRatio", convRatio);
  pw->markIdentityField("convRatio");

  pw->writeString("country", country);
  pw->markIdentityField("country");

  pw->writeDouble("delta", delta);
  pw->markIdentityField("delta");

  pw->writeLong("industry", industry);
  pw->markIdentityField("industry");

  pw->writeLong("issuer", issuer);
  pw->markIdentityField("issuer");

  pw->writeDouble("mktValue", mktValue);
  pw->markIdentityField("mktValue");

  pw->writeDouble("qty", qty);
  pw->markIdentityField("qty");

  pw->writeString("secId", secId);
  pw->markIdentityField("secId");

  pw->writeString("secLinks", secLinks);
  pw->markIdentityField("secLinks");

  pw->writeString("secType", secType);
  pw->markIdentityField("secType");

  pw->writeInt("sharesOutstanding", sharesOutstanding);
  pw->markIdentityField("sharesOutstanding");

  pw->writeString("underlyer", underlyer);
  pw->markIdentityField("underlyer");

  pw->writeLong("volatility", volatility);
  pw->markIdentityField("volatility");

  pw->writeInt("pid", pid);
  pw->markIdentityField("pid");
}

void PositionPdx::fromData( PdxReaderPtr pr ){

  avg20DaysVol = pr->readLong("avg20DaysVol");
  bondRating = pr->readString("bondRating");
  convRatio = pr->readDouble("convRatio");
  country = pr->readString("country");
  delta = pr->readDouble("delta");
  industry = pr->readLong("industry");
  issuer = pr->readLong("issuer");
  mktValue = pr->readDouble("mktValue");
  qty = pr->readDouble("qty");
  secId = pr->readString("secId");
  secLinks = pr->readString("secLinks");
  secType = pr->readString("secType");
  sharesOutstanding = pr->readInt("sharesOutstanding");
  underlyer = pr->readString("underlyer");
  volatility = pr->readLong("volatility");
  pid = pr->readInt("pid");
}

CacheableStringPtr PositionPdx::toString() const {
  char buf[1024];
  sprintf(buf, "PositionPdx Object:[ id=%d ]", this->pid);
  return CacheableString::create( buf );
}

