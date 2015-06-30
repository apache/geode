package com.gemstone.gemfire.codeAnalysis.decode.cp;
import java.io.*;

public class CpNameAndType extends Cp {
    int name_index;     // utf8 unqualified field/method name
    int descriptor_index; // utf8
    CpNameAndType( DataInputStream source ) throws IOException {
        name_index = source.readUnsignedShort();
        descriptor_index = source.readUnsignedShort();
    }
}