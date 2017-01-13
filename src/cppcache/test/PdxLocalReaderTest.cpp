#include <gtest/gtest.h>

#include <PdxType.hpp>
#include <PdxLocalReader.hpp>
#include <PdxLocalWriter.hpp>
#include <PdxTypeRegistry.hpp>

using namespace gemfire;

class MyPdxClass : public PdxSerializable {
 public:
  MyPdxClass();
  ~MyPdxClass();
  virtual void toData(PdxWriterPtr output);
  virtual void fromData(PdxReaderPtr input);
  virtual void setAString(std::string a_string);
  virtual std::string getAString();
  virtual const char *getClassName() const;

  static PdxSerializable *CreateDeserializable();

 private:
  std::string _a_string;
};

MyPdxClass::MyPdxClass() { _a_string = ""; }

void MyPdxClass::setAString(std::string a_string) { _a_string = a_string; }

std::string MyPdxClass::getAString() { return _a_string; }

MyPdxClass::~MyPdxClass() {}

void MyPdxClass::toData(PdxWriterPtr output) {
  output->writeString("name", _a_string.c_str());
}

void MyPdxClass::fromData(PdxReaderPtr input) {
  _a_string = input->readString("name");
}
const char *MyPdxClass::getClassName() const { return "MyPdxClass"; }

PdxSerializable *MyPdxClass::CreateDeserializable() { return new MyPdxClass(); }

TEST(PdxLocalReaderTest, x) {
  MyPdxClass expected, actual;
  DataOutput stream;
  int length = 0;

  expected.setAString("the_expected_string");

  // C++ Client does not require pdxDomainClassName as it is only needed
  // for reflection purposes, which we do not support in C++. We pass in
  // getClassName() for consistency reasons only.
  PdxTypePtr pdx_type_ptr(new PdxType(expected.getClassName(), false));

  // TODO: Refactor static singleton patterns in PdxTypeRegistry so that
  // tests will not interfere with each other.
  PdxTypeRegistry::init();

  // Here we construct a serialized stream of bytes representing MyPdxClass.
  // The stream is later deserialization and validated for consistency.
  PdxLocalWriterPtr writer(new PdxLocalWriter(stream, pdx_type_ptr));
  expected.toData(writer);
  writer->endObjectWriting();
  uint8_t *raw_stream = writer->getPdxStream(length);

  DataInput input(raw_stream, length);
  PdxLocalReaderPtr reader(new PdxLocalReader(input, pdx_type_ptr, length));

  actual.fromData(reader);

  EXPECT_EQ(actual.getAString(), "the_expected_string");
}
