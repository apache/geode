#include "Portfolio.hpp"
#include <malloc.h>

using namespace gemfire;
using namespace testobject;

const char*  Portfolio::secIds[] = { "SUN", "IBM", "YHOO", "GOOG", "MSFT",
      "AOL", "APPL", "ORCL", "SAP", "DELL"};

Portfolio::Portfolio(int32_t i, uint32_t size, CacheableStringArrayPtr nm):
  names(nm){
    ID = i;
    char pkidbuf[1024];
    sprintf(pkidbuf,"%d",i);
    pkid = CacheableString::create(pkidbuf);
    const char* statusStr = (i % 2 == 0) ? "active" : "inactive";
    size_t statusSize = strlen( statusStr ) + 1;
    status = new char[ statusSize ];
    memcpy( status, statusStr, statusSize );
    char buf[100];
    sprintf( buf, "type%d", (i % 3) );
    type = CacheableString::create( buf );
    int numSecIds = sizeof(secIds)/sizeof(char*);
    position1 = new Position(secIds[Position::cnt % numSecIds],
        Position::cnt * 1000);
    if (i % 2 != 0) {
      position2 = new Position(secIds[Position::cnt % numSecIds],
          Position::cnt * 1000);
    }
    else
    {
      position2 = NULLPTR;
    }
    positions = CacheableHashMap::create( );
    positions->insert(CacheableString::create(secIds[Position::cnt % numSecIds]), position1);
    newVal = new uint8_t[size+1];
    memset(newVal,'B',size);
    newVal[size] = '\0';
    newValSize = size;
    creationDate = CacheableDate::create(  );
    arrayNull = NULL;
    arrayZeroSize = NULL;
}

Portfolio::~Portfolio() {
  if ( newVal != NULL ) {
    delete [] newVal;
    newVal = NULL;
  }
  if ( status != NULL ) {
    delete [] status;
    status = NULL;
  }
}

void Portfolio::toData( DataOutput& output ) const {
  output.writeInt(ID);
  output.writeObject(pkid);
  output.writeObject(position1);
  output.writeObject(position2);
  output.writeObject(positions);
  output.writeObject(type);
  output.writeUTF(status);
  output.writeObject(names);
  output.writeBytes(newVal,newValSize+1);
  output.writeObject(creationDate);
  output.writeBytes(arrayNull,0);
  output.writeBytes(arrayZeroSize,0);
}

Serializable* Portfolio::fromData( DataInput& input )
{
  input.readInt(&ID);
  input.readObject(pkid);
  input.readObject(position1);
  input.readObject(position2);
  input.readObject(positions);
  input.readObject(type);
  input.readUTF(&status);
  input.readObject(names);
  input.readBytes(&newVal,&newValSize);
  input.readObject(creationDate);
  int tmp = 0;
  input.readBytes(&arrayNull,&tmp);
  input.readBytes(&arrayZeroSize,&tmp);
  return this;
}

CacheableStringPtr Portfolio::toString() const {
  char idbuf[1024];
  sprintf(idbuf,"PortfolioObject: [ ID=%d",ID);
  char pkidbuf[1024];
  if (pkid != NULLPTR) {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status,
        this->type->toString(), this->pkid->asChar());
  }
  else {
    sprintf(pkidbuf, " status=%s type=%s pkid=%s\n", this->status,
        this->type->toString(), this->pkid->asChar());
  }
  char position1buf[2048];
  if (position1 != NULLPTR) {
    sprintf(position1buf, "\t\t\t  P1: %s", position1->toString()->asChar());
  }
  else {
    sprintf(position1buf, "\t\t\t  P1: %s", "NULL");
  }
  char position2buf[2048];
  if (position2 != NULLPTR) {
    sprintf(position2buf, " P2: %s", position2->toString()->asChar());
  }
  else {
    sprintf(position2buf, " P2: %s ]", "NULL");
  }
  char creationdatebuf[2048];
  if (creationDate != NULLPTR) {
    sprintf(creationdatebuf, "creation Date %s",
        creationDate->toString()->asChar());
  }
  else {
    sprintf(creationdatebuf, "creation Date %s", "NULL");
  }

  char stringBuf[7000];
  sprintf(stringBuf, "%s%s%s%s%s",idbuf,pkidbuf,creationdatebuf,position1buf,position2buf);
  return CacheableString::create( stringBuf );
}
