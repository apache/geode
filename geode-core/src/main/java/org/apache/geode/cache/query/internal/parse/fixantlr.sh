# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
sed -e 's/(char)//' OQLLexer.java >x.java
mv x.java OQLLexer.java

# TODO: this will leave two errors in the resulting file.  So deal
# with it.
sed -e '%s/[ 	]*int _saveIndex;//' OQLLexer.java >x.java
mv x.java OQLLexer.java

sed -e 's/_token==null &&/' OQLLexer.java >x.java
mv x.java OQLLexer.java

# Really, there's only one occurrence and it's the  first one...
sed -e 's/Token _token = null;//' OQLLexer.java >x.java
mv x.java OQLLexer.java

sed -e 's/.*theRetToken.*//' OQLLexer.java >x.java
mv x.java OQLLexer.java

# --------------------------------

sed -e 's/(AST)//g' OQLParser.java >x.java
mv x.java OQLParser.java

# Only one occurrence...
sed -e 's/methodInvocation_AST!=null &&//' OQLParser.java >x.java
mv x.java OQLParser.java

# Ummm...I think this name could change...
# (variable never read)
sed -e 's/AST tmp32_AST = null;//' OQLParser.java >x.java
mv x.java OQLParser.java
sed -e 's/tmp32_AST = //' OQLParser.java >x.java
mv x.java OQLParser.java

sed -e 's/type_AST!=null &&//' OQLParser.java >x.java
mv x.java OQLParser.java
