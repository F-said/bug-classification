/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.net.URL;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;

import junit.framework.AssertionFailedError;

import org.junit.Test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.LoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.ExecType;
import org.apache.pig.impl.builtin.GFAny;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.logicalLayer.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.parser.ParseException ;
import org.apache.pig.impl.util.MultiMap;
import org.apache.pig.test.utils.Identity;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.PigException;


public class TestLogicalPlanBuilder extends junit.framework.TestCase {

    private final Log log = LogFactory.getLog(getClass());
    
    @Test
    public void testQuery1() {
        String query = "foreach (load 'a') generate $1,$2;";
        buildPlan(query);
    }

    @Test
    public void testQuery2() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "(':')) generate $1, 'aoeuaoeu' ;";
        buildPlan(query);
    }

    // TODO FIX Query3 and Query4
    @Test
    public void testQuery3() {
        String query = "foreach (cogroup (load 'a') by $1, (load 'b') by $1) generate org.apache.pig.builtin.AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery4() {
        String query = "foreach (load 'a') generate AVG($1, $2) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery5() {
        String query = "foreach (group (load 'a') ALL) generate $1 ;";
        buildPlan(query);
    }

    
    @Test
    public void testQuery6() {
        String query = "foreach (group (load 'a') by $1) generate group, '1' ;";
        buildPlan(query);
    }

    
    @Test
    public void testQuery7() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "()) generate $1 ;";
        buildPlan(query);
    }

    
    @Test
    public void testQuery10() {
        String query = "foreach (cogroup (load 'a') by ($1), (load 'b') by ($1)) generate $1.$1, $2.$1 ;";
        buildPlan(query);
    }

    // TODO FIX Query11 and Query12
    @Test
    public void testQuery11() {
        String query = " foreach (group (load 'a') by $1, (load 'b') by $2) generate group, AVG($1) ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery12() {
        String query = "foreach (load 'a' using " + PigStorage.class.getName() + "()) generate AVG($1) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery13() {
        String query = "foreach (cogroup (load 'a') ALL) generate group ;";
        buildPlan(query);
    }

    @Test
    public void testQuery14() {
        String query = "foreach (group (load 'a') by ($6, $7)) generate flatten(group) ;";
        buildPlan(query);
    }

    @Test
    public void testQuery15() {
        String query = " foreach (load 'a') generate $1, 'hello', $3 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery100() {
        // test define syntax
        String query = "define FUNC ARITY();";
        LogicalOperator lo = buildPlan(query).getRoots().get(0);
        assertTrue(lo instanceof LODefine);
    }



    @Test
    public void testQueryFail1() {
        String query = " foreach (group (A = load 'a') by $1) generate A.'1' ;";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQueryFail2() {
        String query = "foreach group (load 'a') by $1 generate $1.* ;";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    

    @Test
    public void testQueryFail3() {
        String query = "generate DISTINCT foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQueryFail4() {
        String query = "generate [ORDER BY $0][$3, $4] foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQueryFail5() {
        String query = "generate " + TestApplyFunc.class.getName() + "($2.*) foreach (load 'a');";
        try {
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    
    
    /**
     * User generate functions must be in default package Bug 831620 - fixed
     */
 
    // TODO FIX Query17
    @Test
    public void testQuery17() {
        String query =  "foreach (load 'A')" + "generate " + TestApplyFunc.class.getName() + "($1);";
        buildPlan(query);
    }


    static public class TestApplyFunc extends org.apache.pig.EvalFunc<Tuple> {
        @Override
        public Tuple exec(Tuple input) throws IOException {
            Tuple output = TupleFactory.getInstance().newTuple(input.getAll());
            return output;
        }
    }
    
    
    /**
     * Validate that parallel is parsed correctly Bug 831714 - fixed
     */
    
    @Test
    public void testQuery18() {
        String query = "FOREACH (group (load 'a') ALL PARALLEL 16) generate group;";
        LogicalPlan lp = buildPlan(query);
        LogicalOperator root = lp.getRoots().get(0);   
        
        List<LogicalOperator> listOp = lp.getSuccessors(root);
        
        LogicalOperator lo = listOp.get(0);
        
        if (lo instanceof LOCogroup) {
            assertTrue(((LOCogroup) lo).getRequestedParallelism() == 16);
        } else {
            fail("Error: Unexpected Parse Tree output");
        }  
    }
    
    
    
    
    @Test
    public void testQuery19() {
        buildPlan("a = load 'a';");
        buildPlan("b = filter a by $1 == '3';");
    }
    
    
    @Test
    public void testQuery20() {
        String query = "foreach (load 'a') generate ($1 == '3'? $2 : $3) ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery21() {
        buildPlan("A = load 'a';");
        buildPlan("B = load 'b';");
        buildPlan("foreach (cogroup A by ($1), B by ($1)) generate A, flatten(B.($1, $2, $3));");
    }
    
    @Test
    public void testQuery22() {
        buildPlan("A = load 'a';");
        buildPlan("B = load 'b';");
        buildPlan("C = cogroup A by ($1), B by ($1);");
        String query = "foreach C { " +
                "B = order B by $0; " +
                "generate FLATTEN(A), B.($1, $2, $3) ;" +
                "};" ;
        buildPlan(query);
    }
    
    @Test
    public void testQuery22Fail() {
        buildPlan("A = load 'a';");
        try {
            buildPlan("B = group A by (*, $0);");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Grouping attributes can either be star (*"));
        }
    }
    
    @Test
    public void testQuery23() {
        buildPlan("A = load 'a';");
        buildPlan("B = load 'b';");
        
        buildPlan("C = cogroup A by ($1), B by ($1);");
        
        String query = "foreach C { " +
        "A = Distinct A; " +
        "B = FILTER A BY $1 < 'z'; " +
        //TODO
        //A sequence of filters within a foreach translates to
        //a split statement. Currently it breaks as adding an
        //additional output to the filter fails as filter supports
        //single output
        "C = FILTER A BY $2 == $3;" +
        "B = ARRANGE B BY $1;" +
        "GENERATE A, FLATTEN(B.$0);" +
        "};";
        buildPlan(query);
    }

    @Test
    public void testQuery23Fail() {
        buildPlan("A = load 'a';");
        buildPlan("B = load 'b';");
        try {
            buildPlan("C = group A by (*, $0), B by ($0, $1);");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Grouping attributes can either be star (*"));
        }
    }

    @Test
    public void testQuery24() {
        buildPlan("a = load 'a';");
        
        String query = "foreach a generate (($0 == $1) ? 'a' : $2), $4 ;";
        buildPlan(query);
    }

    @Test
    public void testQuery25() {
        String query = "foreach (load 'a') {" +
                "B = FILTER $0 BY (($1 == $2) AND ('a' < 'b'));" +
                "generate B;" +
                "};";
        buildPlan(query);
    }
    
    
    @Test
    public void testQuery26() {
        String query = "foreach (load 'a') generate  ((NOT (($1 == $2) OR ('a' < 'b'))) ? 'a' : $2), 'x' ;";
        buildPlan(query);
    }
    
    // TODO FIX Query27 and Query28
    @Test
    public void testQuery27() {
        String query =  "foreach (load 'a'){" +
                "A = DISTINCT $3.$1;" +
                " generate " + TestApplyFunc.class.getName() + "($2, $1.($1, $4));" +
                        "};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery28() {
        String query = "foreach (load 'a') generate " + TestApplyFunc.class.getName() + "($2, " + TestApplyFunc.class.getName() + "($2.$3));";
        buildPlan(query);
    }
    
    @Test
    public void testQuery29() {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1);";
        buildPlan(query);
    }


    @Test
    public void testQuery30() {
        String query = "load 'myfile' using " + TestStorageFunc.class.getName() + "() as (col1, col2);";
        buildPlan(query);
    }
    
    
    public static class TestStorageFunc implements LoadFunc{
        public void bindTo(String fileName, BufferedPositionedInputStream is, long offset, long end) throws IOException {
            
        }
        
        public Tuple getNext() throws IOException {
            return null;
        }
        
        public void fieldsToRead(Schema schema) {
            
        }
        
        public DataBag bytesToBag(byte[] b) throws IOException {
            return null;
        }

        public Boolean bytesToBoolean(byte[] b) throws IOException {
            return null;
        }
        
        public String bytesToCharArray(byte[] b) throws IOException {
            return null;
        }
        
        public Double bytesToDouble(byte[] b) throws IOException {
            return null;
        }
        
        public Float bytesToFloat(byte[] b) throws IOException {
            return null;
        }
        
        public Integer bytesToInteger(byte[] b) throws IOException {
            return null;
        }

        public Long bytesToLong(byte[] b) throws IOException {
            return null;
        }

        public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
            return null;
        }

        public Tuple bytesToTuple(byte[] b) throws IOException {
            return null;
        }        

	    public byte[] toBytes(DataBag bag) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(String s) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Double d) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Float f) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Integer i) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Long l) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Map<Object, Object> m) throws IOException {
            return null;
	    }
	
	    public byte[] toBytes(Tuple t) throws IOException {
            return null;
	    }

        /* (non-Javadoc)
         * @see org.apache.pig.LoadFunc#determineSchema(java.lang.String, org.apache.pig.ExecType, org.apache.pig.backend.datastorage.DataStorage)
         */
        public Schema determineSchema(String fileName, ExecType execType,
                DataStorage storage) throws IOException {
            // TODO Auto-generated method stub
            return null;
        }
    }
    
    
    @Test
    public void testQuery31() {
        String query = "load 'myfile' as (col1, col2);";
        buildPlan(query);
    }
    
    @Test
    public void testQuery32() {
        String query = "foreach (load 'myfile' as (col1, col2 : tuple(sub1, sub2), col3 : tuple(bag1))) generate col1 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery33() {
        buildPlan("A = load 'a' as (aCol1, aCol2);");
        buildPlan("B = load 'b' as (bCol1, bCol2);");
        buildPlan("C = cogroup A by (aCol1), B by bCol1;");
        String query = "foreach C generate group, A.aCol1;";
        buildPlan(query);
    }
    
    
    @Test
    //TODO: Nested schemas don't work now. Probably a bug in the new parser.
    public void testQuery34() {
        buildPlan("A = load 'a' as (aCol1, aCol2 : tuple(subCol1, subCol2));");
        buildPlan("A = filter A by aCol2 == '1';");
        buildPlan("B = load 'b' as (bCol1, bCol2);");
        String query = "foreach (cogroup A by (aCol1), B by bCol1 ) generate A.aCol2, B.bCol2 ;";
        buildPlan(query);
    }
    
    
    
    @Test
    public void testQuery35() {
        String query = "foreach (load 'a' as (col1, col2)) generate col1, col2 ;";
        buildPlan(query);
    }
    
    @Test
    public void testQuery36() {
        String query = "foreach (cogroup ( load 'a' as (col1, col2)) by col1) generate $1.(col2, col1);";
        buildPlan(query);
    }
    
    @Test
    public void testQueryFail37() {
        String query = "A = load 'a'; asdasdas";
        try{
            buildPlan(query);
        }catch(AssertionFailedError e){
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery38(){
        String query = "c = cross (load 'a'), (load 'b');";
        buildPlan(query);
    }
    
    
    // TODO FIX Query39 and Query40
    @Test
    public void testQuery39(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by (url,host); ");
        LogicalPlan lp = buildPlan("c = foreach b generate flatten(group.url), SUM(a.rank) as totalRank;");
        buildPlan("d = filter c by totalRank > '10';");
        buildPlan("e = foreach d generate totalRank;");
    }
    
    @Test
    public void testQueryFail39(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by (url,host); ");
        LogicalPlan lp = buildPlan("c = foreach b generate flatten(group.url), SUM(a.rank) as totalRank;");
        buildPlan("d = filter c by totalRank > '10';");
        try {
            buildPlan("e = foreach d generate url;");//url has been falttened and hence the failure
        } catch(AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery40() {
        buildPlan("a = FILTER (load 'a') BY IsEmpty($2);");
        buildPlan("a = FILTER (load 'a') BY (IsEmpty($2) AND ($3 == $2));");
    }
    
    @Test
    public void testQueryFail41() {
        buildPlan("a = load 'a';");
        try {
            buildPlan("b = a as (host,url);");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Currently PIG does not support assigning an existing relation"));
        }
        // TODO
        // the following statement was earlier present
        // eventually when we do allow assignments of the form
        // above, we should test with the line below
        // uncommented
        //buildPlan("foreach b generate host;");
    }
    
    @Test
    public void testQuery42() {
        buildPlan("a = load 'a';");
        buildPlan("b = foreach a generate $0 as url, $1 as rank;");
        buildPlan("foreach b generate url;");
    }

    @Test
    public void testQuery43() {
        buildPlan("a = load 'a' as (url,hitCount);");
        buildPlan("b = load 'a' as (url,rank);");
        buildPlan("c = cogroup a by url, b by url;");
        buildPlan("d = foreach c generate group,flatten(a),flatten(b);");
        buildPlan("e = foreach d generate group, a::url, b::url, b::rank, rank;");
    }

    @Test
    public void testQueryFail43() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        try {
            String query = "c = cogroup a by (name, age), b by (height);";
            buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    } 


    @Test
    public void testQuery44() {
        buildPlan("a = load 'a' as (url, pagerank);");
        buildPlan("b = load 'b' as (url, query, rank);");
        buildPlan("c = cogroup a by (pagerank#'nonspam', url) , b by (rank/'2', url) ;");
        buildPlan("foreach c generate group.url;");
    }

//TODO
//Commented out testQueryFail44 as I am not able to include org.apache.pig.PigServer;
    @Test
    public void testQueryFail44() throws Throwable {
        PigServer pig = null;
        try {
            pig = new PigServer("local");
        } catch (IOException e) {
            assertTrue(false);  // pig server failed for some reason
        }
        pig.registerFunction("myTr",
            new FuncSpec(GFAny.class.getName() + "('tr o 0')"));
        try{
            pig.registerQuery("b = foreach (load 'a') generate myTr(myTr(*));");
        }catch(Exception e){
            return;
        }
        assertTrue(false);
    }
    
    /*
    // Select
    public void testQuery45() {
        buildPlan("A = load 'a' as (url,hitCount);");
        buildPlan("B = select url, hitCount from A;");
        buildPlan("C = select url, hitCount from B;");
    }

    //Select + Join
    public void testQuery46() {
        buildPlan("A = load 'a' as (url,hitCount);");
        buildPlan("B = load 'b' as (url,pageRank);");
        buildPlan("C = select A.url, A.hitCount, B.pageRank from A join B on A.url == B.url;");        
    }

    // Mutliple Joins
    public void testQuery47() {
        buildPlan("A = load 'a' as (url,hitCount);");
        buildPlan("B = load 'b' as (url,pageRank);");
        buildPlan("C = load 'c' as (pageRank, position);");
        buildPlan("B = select A.url, A.hitCount, B.pageRank from (A join B on A.url == B.url) join C on B.pageRank == C.pageRank;");
    }

    // Group
    public void testQuery48() {
        buildPlan("A = load 'a' as (url,hitCount);");        
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url;");
    }

    // Join + Group
    public void testQuery49() {
        buildPlan("A = load 'a' as (url,hitCount);");
        buildPlan("B = load 'b' as (url,pageRank);");
        buildPlan("C = select A.url, AVG(B.pageRank), SUM(A.hitCount) from A join B on A.url == B.url group by A.url;");
    }

    // Group + Having
    public void testQuery50() {
        buildPlan("A = load 'a' as (url,hitCount);");        
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6';");
    }

 // Group + Having + Order
    public void testQuery51() {
        buildPlan("A = load 'a' as (url,hitCount);");        
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url order by A.url;");
    }
    
    // Group + Having + Order
    public void testQuery52() {
        buildPlan("A = load 'a' as (url,hitCount);");        
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6' order by A.url;");
    }

    // Group + Having + Order 2
    public void testQuery53() {
        buildPlan("A = load 'a' as (url,hitCount);");
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.hitCount) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery54() {
        buildPlan("A = load 'a' as (url,hitCount, size);");
        buildPlan("C = select A.url, AVG(A.hitCount) from A group by url having AVG(A.size) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery55() {
        buildPlan("A = load 'a' as (url,hitCount, size);");
        buildPlan("C = select A.url, AVG(A.hitCount), SUM(A.size) from A group by url having AVG(A.size) > '6' order by AVG(A.hitCount);");
    }

    // Group + Having + Order 2
    public void testQuery56() {
        buildPlan("A = load 'a' as (url,hitCount, date);");
        buildPlan("C = select A.url, A.date, SUM(A.hitCount) from A group by url, date having AVG(A.hitCount) > '6' order by A.date;");
    }
    */

    @Test
    public void testQuery57() {
        String query = "foreach (load 'a') generate ($1+$2), ($1-$2), ($1*$2), ($1/$2), ($1%$2), -($1) ;";
        buildPlan(query);
    }

    
    @Test
    public void testQuery58() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        String query = "foreach b {d = a.name; generate group, d;};";
        buildPlan(query);
    } 

	@Test
    public void testQueryFail58(){
        buildPlan("a = load 'a' as (url, host, rank);");
        buildPlan("b = group a by url; ");
        try {
        	LogicalPlan lp = buildPlan("c = foreach b generate group.url;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery59() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = join a by name, b by name;";
        buildPlan(query);
    } 
    
    @Test
    public void testQuery60() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
    } 

    @Test
    public void testQuery61() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = union a,b;";
        buildPlan(query);
    }

    @Test
    public void testQuery62() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
        buildPlan("d = order c by b::name, height, a::gpa;");
        buildPlan("e = order a by name, age, gpa desc;");
        buildPlan("f = order a by $0 asc, age, gpa desc;");
        buildPlan("g = order a by * asc;");
        buildPlan("h = cogroup a by name, b by name;");
        buildPlan("i = foreach h {i1 = order a by *; generate i1;};");
    }

    @Test
    public void testQueryFail62() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
        String query = "c = cross a,b;";
        buildPlan(query);
        try {
        	buildPlan("d = order c by name, b::name, height, a::gpa;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery63() {
        buildPlan("a = load 'a' as (name, details: tuple(age, gpa));");
        buildPlan("b = group a by details;");
        String query = "d = foreach b generate group.age;";
        buildPlan(query);
        buildPlan("e = foreach a generate name, details;");
    }

    @Test
    public void testQueryFail63() {
        String query = "foreach (load 'myfile' as (col1, col2 : (sub1, sub2), col3 : (bag1))) generate col1 ;";
        try {
        	buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery64() {
        buildPlan("a = load 'a' as (name: chararray, details: tuple(age, gpa), mymap: map[]);");
        buildPlan("c = load 'a' as (name, details: bag{mytuple: tuple(age: int, gpa)});");
        buildPlan("b = group a by details;");
        String query = "d = foreach b generate group.age;";
        buildPlan(query);
		buildPlan("e = foreach a generate name, details;");
		buildPlan("f = LOAD 'myfile' AS (garage: bag{tuple1: tuple(num_tools: int)}, links: bag{tuple2: tuple(websites: chararray)}, page: bag{something_stupid: tuple(yeah_double: double)}, coordinates: bag{another_tuple: tuple(ok_float: float, bite_the_array: bytearray, bag_of_unknown: bag{})});");
    }

    @Test
    public void testQueryFail64() {
        String query = "foreach (load 'myfile' as (col1, col2 : bag{age: int})) generate col1 ;";
        try {
        	buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery65() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
		buildPlan("c = cogroup a by (name, age), b by (name, height);");
		buildPlan("d = foreach c generate group.name, a.name as aName, b.name as b::name;");
	}

    @Test
    public void testQueryFail65() {
        buildPlan("a = load 'a' as (name, age, gpa);");
        buildPlan("b = load 'b' as (name, height);");
		buildPlan("c = cogroup a by (name, age), b by (name, height);");
        try {
			buildPlan("d = foreach c generate group.name, a.name, b.height as age, a.age;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
	}

    @Test
    public void testQuery67() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        buildPlan(" b = foreach a generate age, age * 10L, gpa/0.2f, {(16, 4.0e-2, 'hello')};");
    }

    @Test
    public void testQuery68() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate 10, {(16, 4.0e-2, 'hello'), (0.5f, 12l, 'another tuple')};");
    }

    @Test
    public void testQuery69() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate {(16, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, (1))};");
    }

    @Test
    public void testQuery70() {
        buildPlan(" a = load 'input1';");
        buildPlan(" b = foreach a generate [10L#'hello', 4.0e-2#10L, 0.5f#(1), 'world'#42, 42#{('guide')}] as mymap:map[];");
        buildPlan(" c = foreach b generate mymap#10L;");
    }

    @Test
    public void testQueryFail67() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        try {
            buildPlan(" b = foreach a generate age, age * 10L, gpa/0.2f, {16, 4.0e-2, 'hello'};");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQueryFail68() {
        buildPlan(" a = load 'input1' as (name, age, gpa);");
        try {
            buildPlan(" b = foreach a generate {(16 L, 4.0e-2, 'hello'), (0.5f, 'another tuple', 12L, {()})};");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery71() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = foreach x generate $0;");
        buildPlan("c = foreach y generate $1;");
    }

    @Test
    public void testQuery72() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = foreach x generate (int)$0;");
        buildPlan("c = foreach y generate (bag{})$1;");
        buildPlan("d = foreach y generate (int)($1/2);");
        buildPlan("e = foreach y generate (bag{tuple(int, float)})($1/2);");
        buildPlan("f = foreach x generate (tuple(int, float))($1/2);");
        buildPlan("g = foreach x generate (tuple())($1/2);");
        buildPlan("h = foreach x generate (chararray)($1/2);");
    }

    @Test
    public void testQueryFail72() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        try {
            buildPlan("c = foreach y generate (bag)$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
        try {
            buildPlan("c = foreach y generate (bag{int, float})$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
        try {
            buildPlan("c = foreach y generate (tuple)$1;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery73() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = filter x by $0 matches '^fred.*';");
        buildPlan("c = foreach y generate $0, ($0 matches 'yuri.*' ? $1 - 10 : $1);");
    }

    @Test
    public void testQuery74() {
        buildPlan("a = load 'a' as (field1: int, field2: long);");
        buildPlan("b = load 'a' as (field1: bytearray, field2: double);");
        buildPlan("c = group a by field1, b by field1;");
        buildPlan("d = cogroup a by ((field1+field2)*field1), b by field1;");
    }

    @Test
    public void testQuery77() {
        buildPlan("limit (load 'a') 100;");
    }
    
    @Test
    public void testQuery75() {
        buildPlan("a = union (load 'a'), (load 'b'), (load 'c');");
        buildPlan("b = foreach a {generate $0;} parallel 10;");
    }
    
    @Test
    public void testQuery76() {
        buildPlan("split (load 'a') into x if $0 > '7', y if $0 < '7';");
        buildPlan("b = filter x by $0 IS NULL;");
        buildPlan("c = filter y by $0 IS NOT NULL;");
        buildPlan("d = foreach b generate $0, ($1 IS NULL ? 0 : $1 - 7);");
        buildPlan("e = foreach c generate $0, ($1 IS NOT NULL ? $1 - 5 : 0);");
    }

    @Test 
    public void testQuery80() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by age;");
        String query = "d = foreach c {" 
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            //+ "generate group, flatten(cd);"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQuery81() {
        buildPlan("a = load 'input1' using PigStorage() as (name, age, gpa);");
        buildPlan("split a into b if name lt 'f', c if (name gte 'f' and name lte 'h'), d if name gt 'h';");
    }

    @Test
    public void testQueryFail81() {
        buildPlan("a = load 'input1' using PigStorage() as (name, age, gpa);");
        try {
            buildPlan("split a into b if name lt 'f', c if (name ge 'f' and name le 'h'), d if name gt 'h';");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }
    
    @Test
    public void testQuery82() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;"); 
        String query = "c = foreach b {"
            + "c1 = order $1 by *;" 
            + "c2 = $1.$0;" 
            + "generate flatten(c1), c2;"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQueryFail82() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;"); 
        String query = "c = foreach b {"
            + "c1 = order $1 by *;" 
            + "c2 = $1;" 
            + "generate flatten(c1), c2;"
            + "};";
        try {
        buildPlan(query);
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Exception"));
        }
    }

    @Test
    public void testQuery83() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by (name,age);");
        String query = "d = foreach c {" 
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.gpa;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(query);
    }

    @Test
    public void testQuery84() {
        buildPlan("a = load 'input1' as (name, age, gpa);");
        buildPlan("b = filter a by age < '20';");
        buildPlan("c = group b by (name,age);");
        String query = "d = foreach c {"
            + "cf = filter b by gpa < '3.0';"
            + "cp = cf.$2;"
            + "cd = distinct cp;"
            + "co = order cd by gpa;"
            + "generate group, flatten(co);"
            + "};";
        buildPlan(query);
    }
    
    @Test
    public void testQuery85() throws FrontendException {
        LogicalPlan lp;
        buildPlan("a = load 'myfile' as (name, age, gpa);");
        lp = buildPlan("b = group a by (name, age);");
        LOCogroup cogroup = (LOCogroup) lp.getLeaves().get(0);

        Schema.FieldSchema nameFs = new Schema.FieldSchema("name", DataType.BYTEARRAY);
        Schema.FieldSchema ageFs = new Schema.FieldSchema("age", DataType.BYTEARRAY);
        Schema.FieldSchema gpaFs = new Schema.FieldSchema("gpa", DataType.BYTEARRAY);
        
        Schema groupSchema = new Schema(nameFs);
        groupSchema.add(ageFs);
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);
        
        Schema loadSchema = new Schema(nameFs);
        loadSchema.add(ageFs);
        loadSchema.add(gpaFs);

        Schema.FieldSchema bagFs = new Schema.FieldSchema("a", loadSchema, DataType.BAG);
        
        Schema cogroupExpectedSchema = new Schema(groupFs);
        cogroupExpectedSchema.add(bagFs);

        assertTrue(cogroup.getSchema().equals(cogroupExpectedSchema));

        lp = buildPlan("c = foreach b generate group.name, group.age, COUNT(a.gpa);");
        LOForEach foreach  = (LOForEach) lp.getLeaves().get(0);

        Schema foreachExpectedSchema = new Schema(nameFs);
        foreachExpectedSchema.add(ageFs);
        foreachExpectedSchema.add(new Schema.FieldSchema(null, DataType.LONG));

        assertTrue(foreach.getSchema().equals(foreachExpectedSchema));
    }

    @Test
    public void testQuery86() throws FrontendException {
        LogicalPlan lp;
        buildPlan("a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);");
        lp = buildPlan("b = group a by (name, age);");
        LOCogroup cogroup = (LOCogroup) lp.getLeaves().get(0);

        Schema.FieldSchema nameFs = new Schema.FieldSchema("name", DataType.CHARARRAY);
        Schema.FieldSchema ageFs = new Schema.FieldSchema("age", DataType.INTEGER);
        Schema.FieldSchema gpaFs = new Schema.FieldSchema("gpa", DataType.FLOAT);

        Schema groupSchema = new Schema(nameFs);
        groupSchema.add(ageFs);
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);

        Schema loadSchema = new Schema(nameFs);
        loadSchema.add(ageFs);
        loadSchema.add(gpaFs);

        Schema.FieldSchema bagFs = new Schema.FieldSchema("a", loadSchema, DataType.BAG);

        Schema cogroupExpectedSchema = new Schema(groupFs);
        cogroupExpectedSchema.add(bagFs);

        assertTrue(cogroup.getSchema().equals(cogroupExpectedSchema));

    }

    @Test
    public void testQuery87() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;");
        LogicalPlan lp = buildPlan("c = foreach b {c1 = order $1 by $1; generate flatten(c1); };");
        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        LogicalPlan nestedPlan = foreach.getForEachPlans().get(0);
        LOProject sortInput = (LOProject)nestedPlan.getRoots().get(0);
        LOSort nestedSort = (LOSort)nestedPlan.getSuccessors(sortInput).get(0);
        LogicalPlan sortPlan = nestedSort.getSortColPlans().get(0);
        assertTrue(sortPlan.getLeaves().size() == 1);
    }

    @Test
    public void testQuery88() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = group a by $0;");
        LogicalPlan lp = buildPlan("c = order b by $1 ;");
        LOSort sort = (LOSort)lp.getLeaves().get(0);
        LOProject project1 = (LOProject) sort.getSortColPlans().get(0).getLeaves().get(0) ;
        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(sort).get(0) ;
        assertEquals(project1.getExpression(), cogroup) ;
    }

    @Test
    public void testQuery89() {
        buildPlan("a = load 'myfile';");
        buildPlan("b = foreach a generate $0, $100;");
        buildPlan("c = load 'myfile' as (i: int);");
        buildPlan("d = foreach c generate $0 as zero, i;");
    }

    @Test
    public void testQueryFail89() {
        buildPlan("c = load 'myfile' as (i: int);");
        try {
            buildPlan("d = foreach c generate $0, $5;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Out of bound access"));
        }
    }

    @Test
    public void testQuery90() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);");
        buildPlan("b = group a by (name, age);");

        //the first element in group, i.e., name is renamed as myname
        lp = buildPlan("c = foreach b generate flatten(group) as (myname), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("myname: chararray, age: int, mycount: long")));

        //the first and second elements in group, i.e., name and age are renamed as myname and myage
        lp = buildPlan("c = foreach b generate flatten(group) as (myname, myage), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("myname: chararray, myage: int, mycount: long")));

        //the schema of group is unchanged
        lp = buildPlan("c = foreach b generate flatten(group) as (), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("group::name: chararray, group::age: int, mycount: long")));

        //the first element in group, i.e., name is renamed as myname 
        lp = buildPlan("c = foreach b generate flatten(group) as myname, COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("myname: chararray, age: int, mycount: long")));

        //group is renamed as mygroup
        lp = buildPlan("c = foreach b generate group as mygroup, COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("mygroup:(name: chararray, age: int), mycount: long")));

        //group is renamed as mygroup and the first element is renamed as myname
        lp = buildPlan("c = foreach b generate group as mygroup:(myname), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("mygroup:(myname: chararray, age: int), mycount: long")));

        //group is renamed as mygroup and the elements are renamed as myname and myage
        lp = buildPlan("c = foreach b generate group as mygroup:(myname, myage), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("mygroup:(myname: chararray, myage: int), mycount: long")));

        //group is renamed to mygroup as the tuple schema is empty
        lp = buildPlan("c = foreach b generate group as mygroup:(), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("mygroup:(name: chararray, age: int), mycount: long")));

        //setting the schema of flattened bag that has no schema with the user defined schema
        buildPlan("c = load 'another_file';");
        buildPlan("d = cogroup a by $0, c by $0;");
        lp = buildPlan("e = foreach d generate flatten(DIFF(a, c)) as (x, y, z), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("x: bytearray, y: bytearray, z: bytearray, mycount: long")));

        //setting the schema of flattened bag that has no schema with the user defined schema
        buildPlan("c = load 'another_file';");
        buildPlan("d = cogroup a by $0, c by $0;");
        lp = buildPlan("e = foreach d generate flatten(DIFF(a, c)) as (x: int, y: float, z), COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("x: int, y: float, z: bytearray, mycount: long")));

        //setting the schema of flattened bag that has no schema with the user defined schema
        buildPlan("c = load 'another_file';");
        buildPlan("d = cogroup a by $0, c by $0;");
        lp = buildPlan("e = foreach d generate flatten(DIFF(a, c)) as x, COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("x: bytearray, mycount: long")));

        //setting the schema of flattened bag that has no schema with the user defined schema
        buildPlan("c = load 'another_file';");
        buildPlan("d = cogroup a by $0, c by $0;");
        lp = buildPlan("e = foreach d generate flatten(DIFF(a, c)) as x: int, COUNT(a) as mycount;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(foreach.getSchema().equals(Util.getSchemaFromString("x: int, mycount: long")));

    }

    @Test
    public void testQueryFail90() throws FrontendException, ParseException {
        buildPlan("a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);");
        buildPlan("b = group a by (name, age);");

        try {
            buildPlan("c = foreach b generate group as mygroup:(myname, myage, mygpa), COUNT(a) as mycount;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Schema size mismatch"));
        }

        try {
            buildPlan("c = foreach b generate group as mygroup:(myname: int, myage), COUNT(a) as mycount;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }

        try {
            buildPlan("c = foreach b generate group as mygroup:(myname, myage: chararray), COUNT(a) as mycount;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }

        try {
            buildPlan("c = foreach b generate group as mygroup:{t: (myname, myage)}, COUNT(a) as mycount;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Type mismatch"));
        }

        try {
            buildPlan("c = foreach b generate flatten(group) as (myname, myage, mygpa), COUNT(a) as mycount;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Schema size mismatch"));
        }
    }
        
    @Test
    public void testQuery91() {
        buildPlan("a = load 'myfile' as (name:Chararray, age:Int, gpa:Float);");
        buildPlan("b = group a by name;");
        buildPlan("c = foreach b generate SUM(a.age) + SUM(a.gpa);");
    }

    @Test
    public void testQuery92() {
        buildPlan("a = load 'myfile' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        String query = "c = foreach b { "
        + " alias = name#'alias'; "
        + " af = alias#'first'; "
        + " al = alias#'last'; "
        + " generate SUM(a.age) + SUM(a.gpa); "
        + "};";
    }

    @Test
    public void testQuery93() throws FrontendException, ParseException {
        buildPlan("a = load 'one' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        buildPlan("c = foreach b generate flatten(a);");
        buildPlan("d = foreach c generate name;");
        // test that we can refer to "name" field and not a::name
        buildPlan("e = foreach d generate name;");
    }
    
    @Test
    public void testQueryFail93() throws FrontendException, ParseException {
        buildPlan("a = load 'one' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        buildPlan("c = foreach b generate flatten(a);");
        buildPlan("d = foreach c generate name;");
        // test that we can refer to "name" field and a::name
        buildPlan("e = foreach d generate a::name;");
    }
    
    @Test
    public void testQuery94() throws FrontendException, ParseException {
        buildPlan("a = load 'one' as (name, age, gpa);");
        buildPlan("b = load 'two' as (name, age, somethingelse);");
        buildPlan("c = cogroup a by name, b by name;");
        buildPlan("d = foreach c generate flatten(a), flatten(b);");
        // test that we can refer to "a::name" field and not name
        // test that we can refer to "b::name" field and not name
        buildPlan("e = foreach d generate a::name, b::name;");
        // test that we can refer to gpa and somethingelse
        buildPlan("f = foreach d generate gpa, somethingelse, a::gpa, b::somethingelse;");
        
    }
    
    @Test
    public void testQueryFail94() throws FrontendException, ParseException {
        buildPlan("a = load 'one' as (name, age, gpa);");
        buildPlan("b = load 'two' as (name, age, somethingelse);");
        buildPlan("c = cogroup a by name, b by name;");
        buildPlan("d = foreach c generate flatten(a), flatten(b);");
        // test that we can refer to "a::name" field and not name
        try {
            buildPlan("e = foreach d generate name;");
        } catch (AssertionFailedError e) {
            assertTrue(e.getMessage().contains("Found more than one match:"));
        }
    }

    @Test
    public void testQuery95() throws FrontendException, ParseException {
        buildPlan("a = load 'myfile' as (name, age, gpa);");
        buildPlan("b = group a by name;");
        LogicalPlan lp = buildPlan("c = foreach b {d = order a by $1; generate flatten(d), MAX(a.age) as max_age;};");
        LOForEach foreach = (LOForEach) lp.getLeaves().get(0);
        LOCogroup cogroup = (LOCogroup) lp.getPredecessors(foreach).get(0);
        Schema.FieldSchema bagFs = new Schema.FieldSchema("a", Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray"), DataType.BAG);
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", DataType.BYTEARRAY);
        Schema cogroupExpectedSchema = new Schema();
        cogroupExpectedSchema.add(groupFs);
        cogroupExpectedSchema.add(bagFs);
        assertTrue(Schema.equals(cogroup.getSchema(), cogroupExpectedSchema, false, false));
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray, max_age: double"), false, true));
    }

    @Test
    public void testQuery96() throws FrontendException, ParseException {
        buildPlan("a = load 'input' as (name, age, gpa);");
        buildPlan("b = filter a by age < 20;");
        buildPlan("c = group b by age;");
        String query = "d = foreach c {"
        + "cf = filter b by gpa < 3.0;"
        + "cd = distinct cf.gpa;"
        + "co = order cd by $0;"
        + "generate group, flatten(co);"
        + "};";
        LogicalPlan lp = buildPlan(query);

        LOForEach foreach = (LOForEach)lp.getLeaves().get(0);
        ArrayList<LogicalPlan> foreachPlans = foreach.getForEachPlans();
        LogicalPlan flattenPlan = foreachPlans.get(1);
        LogicalOperator project = flattenPlan.getLeaves().get(0);
        assertTrue(project instanceof LOProject);
        LogicalOperator sort = flattenPlan.getPredecessors(project).get(0);
        assertTrue(sort instanceof LOSort);
        LogicalOperator distinct = flattenPlan.getPredecessors(sort).get(0);
        assertTrue(distinct instanceof LODistinct);

        //testing the presence of the nested foreach
        LogicalOperator nestedForeach = flattenPlan.getPredecessors(distinct).get(0);
        assertTrue(nestedForeach instanceof LOForEach);
        LogicalPlan nestedForeachPlan = ((LOForEach)nestedForeach).getForEachPlans().get(0);
        LogicalOperator nestedProject = nestedForeachPlan.getRoots().get(0);
        assertTrue(nestedProject instanceof LOProject);
        assertTrue(((LOProject)nestedProject).getCol() == 2);

        //testing the filter inner plan for the absence of the project connected to project
        LogicalOperator filter = flattenPlan.getPredecessors(nestedForeach).get(0);
        assertTrue(filter instanceof LOFilter);
        LogicalPlan comparisonPlan = ((LOFilter)filter).getComparisonPlan();
        LOLesserThan lessThan = (LOLesserThan)comparisonPlan.getLeaves().get(0);
        LOProject filterProject = (LOProject)lessThan.getLhsOperand();
        assertTrue(null == comparisonPlan.getPredecessors(filterProject));
    }

    @Test
    public void testQuery97() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate 1;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: int"), false, true));

        lp = buildPlan("b = foreach a generate 1L;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: long"), false, true));

        lp = buildPlan("b = foreach a generate 1.0;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: double"), false, true));

        lp = buildPlan("b = foreach a generate 1.0f;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: float"), false, true));

        lp = buildPlan("b = foreach a generate 'hello';");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("x: chararray"), false, true));
    }

    @Test
    public void testQuery98() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate (1);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: int)"), false, true));

        lp = buildPlan("b = foreach a generate (1L);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: long)"), false, true));

        lp = buildPlan("b = foreach a generate (1.0);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: double)"), false, true));

        lp = buildPlan("b = foreach a generate (1.0f);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: float)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello');");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello', 1, 1L, 1.0f, 1.0);");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray, y: int, z: long, a: float, b: double)"), false, true));

        lp = buildPlan("b = foreach a generate ('hello', {(1), (1.0)});");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("t:(x: chararray, ib:{it:(d: double)})"), false, true));

    }

    @Test
    public void testQuery99() throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (2, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: int, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1L, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: long, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: float, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1L, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: float, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1L, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0f, 'hello'), (1.0, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0, 'hello'), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:(x: double, y: chararray)}"), false, true));

        lp = buildPlan("b = foreach a generate {(1.0, 'hello', 3.14), (1.0f, 'world')};");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("b:{t:()}"), false, true));

    }

    @Test
    public void testQuery101() {
        // test usage of an alias from define
        String query = "define FUNC ARITY();";
        buildPlan(query);

        query = "foreach (load 'data') generate FUNC($0);";
        buildPlan(query);
    }

    @Test
    public void testQuery102() {
        // test basic store
        buildPlan("a = load 'a';");
        buildPlan("store a into 'out';");
    }

    @Test
    public void testQuery103() {
        // test store with store function
        buildPlan("a = load 'a';");
        buildPlan("store a into 'out' using PigStorage();");
    }

    @Test
    public void testQuery104() {
        // check that a field alias can be referenced
        // by unambiguous free form alias, fully qualified alias
        // and partially qualified unambiguous alias
        buildPlan( "a = load 'st10k' as (name, age, gpa);" );
        buildPlan( "b = group a by name;" );
        buildPlan("c = foreach b generate flatten(a);" );
        buildPlan("d = filter c by name != 'fred';" );
        buildPlan("e = group d by name;" );
        buildPlan("f = foreach e generate flatten(d);" );
        buildPlan("g = foreach f generate name, d::a::name, a::name;");

    }

    @Test
    public void testQuery105() {
        // test that the alias "group" can be used
        // after a flatten(group)
        buildPlan( "a = load 'st10k' as (name, age, gpa);" );
        buildPlan("b = group a by name;" );
        buildPlan("c = foreach b generate flatten(group), COUNT(a) as cnt;" );
        buildPlan("d = foreach c generate group;");

    }

    @Test
    public void testQuery106()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate *;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        assertTrue(Schema.equals(foreach.getSchema(), Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray"), false, true));

    }

    @Test
    public void testQuery107()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one';");

        lp = buildPlan("b = foreach a generate *;");
        foreach = (LOForEach) lp.getLeaves().get(0);
        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);

        assertTrue(checkPlanForProjectStar(foreachPlan));

    }

    @Test
    public void testQuery108()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOCogroup cogroup;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = group a by *;");
        cogroup = (LOCogroup) lp.getLeaves().get(0);
        Schema groupSchema = Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray");
        Schema bagASchema = Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray");
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);
        Schema.FieldSchema bagAFs = new Schema.FieldSchema("a", bagASchema, DataType.BAG);
        Schema expectedSchema = new Schema(groupFs);
        expectedSchema.add(bagAFs);
        assertTrue(Schema.equals(cogroup.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testQuery109()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOCogroup cogroup;

        buildPlan("a = load 'one' as (name, age, gpa);");
        buildPlan("b = load 'two' as (first_name, enrol_age, high_school_gpa);");

        lp = buildPlan("c = group a by *, b by *;");
        cogroup = (LOCogroup) lp.getLeaves().get(0);
        Schema groupSchema = Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray");
        Schema bagASchema = Util.getSchemaFromString("name: bytearray, age: bytearray, gpa: bytearray");
        Schema bagBSchema = Util.getSchemaFromString("first_name: bytearray, enrol_age: bytearray, high_school_gpa: bytearray");
        Schema.FieldSchema groupFs = new Schema.FieldSchema("group", groupSchema, DataType.TUPLE);
        Schema.FieldSchema bagAFs = new Schema.FieldSchema("a", bagASchema, DataType.BAG);
        Schema.FieldSchema bagBFs = new Schema.FieldSchema("b", bagBSchema, DataType.BAG);
        Schema expectedSchema = new Schema(groupFs);
        expectedSchema.add(bagAFs);
        expectedSchema.add(bagBFs);
        assertTrue(Schema.equals(cogroup.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testQuery110()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOLoad load;
        LOCogroup cogroup;

        buildPlan("a = load 'one' as (name, age, gpa);");
        lp = buildPlan("b = load 'two';");

        load = (LOLoad) lp.getLeaves().get(0);

        lp = buildPlan("c = cogroup a by $0, b by *;");
        cogroup = (LOCogroup) lp.getLeaves().get(0);

        MultiMap<LogicalOperator, LogicalPlan> mapGByPlans = cogroup.getGroupByPlans();
        LogicalPlan cogroupPlan = (LogicalPlan)(mapGByPlans.get(load).toArray())[0];
        assertTrue(checkPlanForProjectStar(cogroupPlan) == true);

    }

    @Test
    public void testQuery111()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOSort sort;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = order a by *;");
        sort = (LOSort) lp.getLeaves().get(0);

        for(LogicalPlan sortPlan: sort.getSortColPlans()) {
            assertTrue(checkPlanForProjectStar(sortPlan) == false);
        }

    }

    @Test
    public void testQuery112()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one' as (name, age, gpa);");

        buildPlan("b = group a by *;");
        lp = buildPlan("c = foreach b {a1 = order a by *; generate a1;};");
        foreach = (LOForEach) lp.getLeaves().get(0);

        for(LogicalPlan foreachPlan: foreach.getForEachPlans()) {
            assertTrue(checkPlanForProjectStar(foreachPlan) == true);
        }

        LogicalPlan foreachPlan = foreach.getForEachPlans().get(0);

        sort = (LOSort)foreachPlan.getPredecessors(foreachPlan.getLeaves().get(0)).get(0);

        for(LogicalPlan sortPlan: sort.getSortColPlans()) {
            assertTrue(checkPlanForProjectStar(sortPlan) == true);
        }

    }

    @Test
    public void testQuery113()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a {exp1 = age + gpa; exp2 = exp1 + age; generate exp1, exp2;};");
        foreach = (LOForEach) lp.getLeaves().get(0);

        for(LogicalPlan foreachPlan: foreach.getForEachPlans()) {
            printPlan(foreachPlan);
            assertTrue(checkPlanForProjectStar(foreachPlan) == false);
        }

    }

    @Test
    public void testQuery114()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate " + Identity.class.getName() + "(name, age);");
        foreach = (LOForEach) lp.getLeaves().get(0);

        Schema s = new Schema();
        s.add(new Schema.FieldSchema("name", DataType.BYTEARRAY));
        s.add(new Schema.FieldSchema("age", DataType.BYTEARRAY));
        Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, s, DataType.TUPLE);
        Schema expectedSchema = new Schema(tupleFs);
        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testQuery115()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one' as (name, age, gpa);");

        lp = buildPlan("b = foreach a generate " + Identity.class.getName() + "(*);");
        foreach = (LOForEach) lp.getLeaves().get(0);

        Schema s = new Schema();
        s.add(new Schema.FieldSchema("name", DataType.BYTEARRAY));
        s.add(new Schema.FieldSchema("age", DataType.BYTEARRAY));
        s.add(new Schema.FieldSchema("gpa", DataType.BYTEARRAY));
        Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, s, DataType.TUPLE);
        Schema expectedSchema = new Schema(tupleFs);
        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testQuery116()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one';");

        lp = buildPlan("b = foreach a generate " + Identity.class.getName() + "($0, $1);");
        foreach = (LOForEach) lp.getLeaves().get(0);

        Schema s = new Schema();
        s.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        s.add(new Schema.FieldSchema(null, DataType.BYTEARRAY));
        Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, s, DataType.TUPLE);
        Schema expectedSchema = new Schema(tupleFs);
        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testQuery117()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;
        LOSort sort;

        buildPlan("a = load 'one';");

        lp = buildPlan("b = foreach a generate " + Identity.class.getName() + "(*);");
        foreach = (LOForEach) lp.getLeaves().get(0);

        Schema.FieldSchema tupleFs = new Schema.FieldSchema(null, null, DataType.TUPLE);
        Schema expectedSchema = new Schema(tupleFs);
        assertTrue(Schema.equals(foreach.getSchema(), expectedSchema, false, true));

    }

    @Test
    public void testNullConsArithExprs() {
        buildPlan("a = load 'a' as (x:int, y:double);" );
        buildPlan("b = foreach a generate x + null, x * null, x / null, x - null, null % x, " +
                "y + null, y * null, y / null, y - null;"
        );
    }

    @Test
    public void testNullConsBincond1() {
        buildPlan("a = load 'a' as (x:int, y:double);" );
        buildPlan("b = foreach a generate (2 > 1? null : 1), ( 2 < 1 ? null : 1), " +
                "(2 > 1 ? 1 : null), ( 2 < 1 ? 1 : null);"
        );
    }

    @Test
    public void testNullConsBincond2() {
        buildPlan("a = load 'a' as (x:int, y:double);" );
        buildPlan("b = foreach a generate (null is null ? 1 : 2), ( null is not null ? 2 : 1);");
    }

    @Test
    public void testNullConsForEachGenerate() {
        buildPlan("a = load 'a' as (x:int, y:double);" );
        buildPlan("b = foreach a generate x, null, y, null;");

    }

    @Test
    public void testNullConsOuterJoin() {
        buildPlan("a = load 'a' as (x:int, y:chararray);" );
        buildPlan("b = load 'b' as (u:int, v:chararray);" );
        buildPlan("c = cogroup a by x, b by u;" );
        buildPlan("d = foreach c generate flatten((SIZE(a) == 0 ? null : a)), " +
                "flatten((SIZE(b) == 0 ? null : b));"
        );
    }

    @Test
    public void testNullConsConcatSize() {
        buildPlan("a = load 'a' as (x:int, y:double, str:chararray);" );
        buildPlan("b = foreach a generate SIZE(null), CONCAT(str, null), " + 
                "CONCAT(null, str);"
        );
    }

    @Test
    public void testFilterUdfDefine() {
        buildPlan("define isempty IsEmpty();");
        buildPlan("a = load 'a' as (x:int, y:double, str:chararray);");
        buildPlan("b = filter a by isempty(*);");
    }

    @Test
    public void testLoadUdfDefine() {
        buildPlan("define PS PigStorage();");
        buildPlan("a = load 'a' using PS as (x:int, y:double, str:chararray);" );
        buildPlan("b = filter a by IsEmpty(*);");
    }

    @Test
    public void testLoadUdfConstructorArgDefine() {
        buildPlan("define PS PigStorage(':');");
        buildPlan("a = load 'a' using PS as (x:int, y:double, str:chararray);" );
        buildPlan("b = filter a by IsEmpty(*);");
    }

    @Test
    public void testStoreUdfDefine() {
        buildPlan( "define PS PigStorage();");
        buildPlan("a = load 'a' using PS as (x:int, y:double, str:chararray);" );
        buildPlan("b = filter a by IsEmpty(*);" );
        buildPlan("store b into 'x' using PS;");
    }

    @Test
    public void testStoreUdfConstructorArgDefine() {
        buildPlan( "define PS PigStorage(':');");
        buildPlan(" a = load 'a' using PS as (x:int, y:double, str:chararray);" );
        buildPlan(" b = filter a by IsEmpty(*);" );
        buildPlan(" store b into 'x' using PS;") ;

    }

    @Test
    public void testCastAlias() {
        buildPlan("a = load 'one.txt' as (x,y); ");
        buildPlan("b =  foreach a generate (int)x, (double)y;");
        buildPlan("c = group b by x;");
    }

    @Test
    public void testCast() {
        buildPlan("a = load 'one.txt' as (x,y); " );
        buildPlan("b = foreach a generate (int)$0, (double)$1;" ); 
        buildPlan("c = group b by $0;");
    }


    @Test
    public void testTokenizeSchema()  throws FrontendException, ParseException {
        LogicalPlan lp;
        LOForEach foreach;

        buildPlan("a = load 'one' as (f1: chararray);");
        lp = buildPlan("b = foreach a generate TOKENIZE(f1);");
        foreach = (LOForEach) lp.getLeaves().get(0);

        Schema.FieldSchema tokenFs = new Schema.FieldSchema("token", 
                DataType.CHARARRAY); 
        Schema tupleSchema = new Schema(tokenFs);

        Schema.FieldSchema tupleFs;
        tupleFs = new Schema.FieldSchema("tuple_of_tokens", tupleSchema,
                DataType.TUPLE);

        Schema bagSchema = new Schema(tupleFs);
        bagSchema.setTwoLevelAccessRequired(true);
        Schema.FieldSchema bagFs = new Schema.FieldSchema(
                    "bag_of_tokenTuples",bagSchema, DataType.BAG);
        
        assertTrue(Schema.equals(foreach.getSchema(), new Schema(bagFs), false, true));
    }
    
    private void printPlan(LogicalPlan lp) {
        LOPrinter graphPrinter = new LOPrinter(System.err, lp);
        System.err.println("Printing the logical plan");
        try {
            graphPrinter.visit();
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
        System.err.println();
    }
    
    private boolean checkPlanForProjectStar(LogicalPlan lp) {
        List<LogicalOperator> leaves = lp.getLeaves();

        for(LogicalOperator op: leaves) {
            if(op instanceof LOProject) {
                if(((LOProject) op).isStar()) {
                    return true;
                }
            }
        }

        return false;
    }

    // Helper Functions
    
    // Helper Functions
    // =================
    public LogicalPlan buildPlan(String query) {
        return buildPlan(query, LogicalPlanBuilder.class.getClassLoader());
    }

    public LogicalPlan buildPlan(String query, ClassLoader cldr) {
        LogicalPlanBuilder.classloader = cldr;
        LogicalPlanBuilder builder = new LogicalPlanBuilder(pigContext); //

        try {
            LogicalPlan lp = builder.parse("Test-Plan-Builder",
                                           query,
                                           aliases,
                                           logicalOpTable,
                                           aliasOp);
            List<LogicalOperator> roots = lp.getRoots();
            
            if(roots.size() > 0) {
                for(LogicalOperator op: roots) {
                    if (!(op instanceof LOLoad) && !(op instanceof LODefine)){
                        throw new Exception("Cannot have a root that is not the load or define operator. Found " + op.getClass().getName());
                    }
                }
            }
            
            //System.err.println("Query: " + query);
            
            assertNotNull(lp != null);
            return lp;
        } catch (IOException e) {
            // log.error(e);
            //System.err.println("IOException Stack trace for query: " + query);
            //e.printStackTrace();
            PigException pe = LogUtils.getPigException(e);
            fail("IOException: " + (pe == null? e.getMessage(): pe.getMessage()));
        } catch (Exception e) {
            log.error(e);
            //System.err.println("Exception Stack trace for query: " + query);
            //e.printStackTrace();
            PigException pe = LogUtils.getPigException(e);
            fail(e.getClass().getName() + ": " + (pe == null? e.getMessage(): pe.getMessage()) + " -- " + query);
        }
        return null;
    }
    
    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> logicalOpTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    PigContext pigContext = new PigContext(ExecType.LOCAL, new Properties());
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.pig.backend.hadoop.executionengine;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketImplFactory;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobTracker;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecPhysicalOperator;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.executionengine.util.ExecTools;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.datastorage.HDataStorage;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.LogToPhyTranslationVisitor;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MapReduceLauncher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PlanPrinter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.shock.SSHSocketImplFactory;


public class HExecutionEngine implements ExecutionEngine {
    
    private static final String HOD_SERVER = "hod.server";
    public static final String JOB_TRACKER_LOCATION = "mapred.job.tracker";
    private static final String FILE_SYSTEM_LOCATION = "fs.default.name";
    
    private final Log log = LogFactory.getLog(getClass());
    private static final String LOCAL = "local";
    
    private StringBuilder hodParams = null;
    
    protected PigContext pigContext;
    
    protected DataStorage ds;
    
    protected JobClient jobClient;

    // key: the operator key from the logical plan that originated the physical plan
    // val: the operator key for the root of the phyisical plan
    protected Map<OperatorKey, OperatorKey> logicalToPhysicalKeys;
    
    protected Map<OperatorKey, ExecPhysicalOperator> physicalOpTable;
    
    // map from LOGICAL key to into about the execution
    protected Map<OperatorKey, MapRedResult> materializedResults;
    
    public HExecutionEngine(PigContext pigContext) {
        this.pigContext = pigContext;
        this.logicalToPhysicalKeys = new HashMap<OperatorKey, OperatorKey>();
        this.physicalOpTable = new HashMap<OperatorKey, ExecPhysicalOperator>();
        this.materializedResults = new HashMap<OperatorKey, MapRedResult>();
        
        this.ds = null;
        
        // to be set in the init method
        this.jobClient = null;
    }
    
    public JobClient getJobClient() {
        return this.jobClient;
    }
    
    public Map<OperatorKey, MapRedResult> getMaterializedResults() {
        return this.materializedResults;
    }
    
    public Map<OperatorKey, ExecPhysicalOperator> getPhysicalOpTable() {
        return this.physicalOpTable;
    }
    
    
    public DataStorage getDataStorage() {
        return this.ds;
    }

    public void init() throws ExecException {
        init(this.pigContext.getProperties());
    }
    
    public void init(Properties properties) throws ExecException {
        //First set the ssh socket factory
        setSSHFactory();
        
        String hodServer = properties.getProperty(HOD_SERVER);
        String cluster = null;
        String nameNode = null;
        Configuration configuration = null;
    
        if (hodServer != null && hodServer.length() > 0) {
            String hdfsAndMapred[] = doHod(hodServer, properties);
            properties.setProperty(FILE_SYSTEM_LOCATION, hdfsAndMapred[0]);
            properties.setProperty(JOB_TRACKER_LOCATION, hdfsAndMapred[1]);
        }
        else {
            
            // We need to build a configuration object first in the manner described below
            // and then get back a properties object to inspect the JOB_TRACKER_LOCATION
            // and FILE_SYSTEM_LOCATION. The reason to do this is if we looked only at
            // the existing properties object, we may not get the right settings. So we want
            // to read the configurations in the order specified below and only then look
            // for JOB_TRACKER_LOCATION and FILE_SYSTEM_LOCATION.
            
            // Hadoop by default specifies two resources, loaded in-order from the classpath:
            // 1. hadoop-default.xml : Read-only defaults for hadoop.
            // 2. hadoop-site.xml: Site-specific configuration for a given hadoop installation.
            // Now add the settings from "properties" object to override any existing properties
            // All of the above is accomplished in the method call below
           
            JobConf jobConf = new JobConf();
            jobConf.addResource("pig-cluster-hadoop-site.xml");
            
            //the method below alters the properties object by overriding the
            //hadoop properties with the values from properties and recomputing
            //the properties
            recomputeProperties(jobConf, properties);
            
            configuration = ConfigurationUtil.toConfiguration(properties);            
            properties = ConfigurationUtil.toProperties(configuration);
            cluster = properties.getProperty(JOB_TRACKER_LOCATION);
            nameNode = properties.getProperty(FILE_SYSTEM_LOCATION);
            
            if (cluster != null && cluster.length() > 0) {
                if(!cluster.contains(":") && !cluster.equalsIgnoreCase(LOCAL)) {
                    cluster = cluster + ":50020";
                }
                properties.setProperty(JOB_TRACKER_LOCATION, cluster);
            }

            if (nameNode!=null && nameNode.length() > 0) {
                if(!nameNode.contains(":")  && !nameNode.equalsIgnoreCase(LOCAL)) {
                    nameNode = nameNode + ":8020";
                }
                properties.setProperty(FILE_SYSTEM_LOCATION, nameNode);
            }
        }
     
        log.info("Connecting to hadoop file system at: "  + (nameNode==null? LOCAL: nameNode) )  ;
        ds = new HDataStorage(properties);
                
        // The above HDataStorage constructor sets DEFAULT_REPLICATION_FACTOR_KEY in properties.
        // So we need to reconstruct the configuration object for the non HOD case
        // In the HOD case, this is the first time the configuration object will be created
        configuration = ConfigurationUtil.toConfiguration(properties);
        
            
        if(cluster != null && !cluster.equalsIgnoreCase(LOCAL)){
                log.info("Connecting to map-reduce job tracker at: " + properties.get(JOB_TRACKER_LOCATION));
        }

        try {
            // Set job-specific configuration knobs
            jobClient = new JobClient(new JobConf(configuration));
        }
        catch (IOException e) {
            int errCode = 6009;
            String msg = "Failed to create job client:" + e.getMessage();
            throw new ExecException(msg, errCode, PigException.BUG, e);
        }
    }

    public void close() throws ExecException {
        closeHod(pigContext.getProperties().getProperty("hod.server"));
    }
        
    public Properties getConfiguration() throws ExecException {
        return this.pigContext.getProperties();
    }
        
    public void updateConfiguration(Properties newConfiguration) 
            throws ExecException {
        init(newConfiguration);
    }
        
    public Map<String, Object> getStatistics() throws ExecException {
        throw new UnsupportedOperationException();
    }

    public PhysicalPlan compile(LogicalPlan plan,
                                Properties properties) throws ExecException {
        if (plan == null) {
            int errCode = 2041;
            String msg = "No Plan to compile";
            throw new ExecException(msg, errCode, PigException.BUG);
        }

        try {
            LogToPhyTranslationVisitor translator = 
                new LogToPhyTranslationVisitor(plan);
            translator.setPigContext(pigContext);
            translator.visit();
            return translator.getPhysicalPlan();
        } catch (VisitorException ve) {
            int errCode = 2042;
            String msg = "Internal error. Unable to translate logical plan to physical plan.";
            throw new ExecException(msg, errCode, PigException.BUG, ve);
        }
    }

    public ExecJob execute(PhysicalPlan plan,
                           String jobName) throws ExecException {
        try {
            FileSpec spec = ExecTools.checkLeafIsStore(plan, pigContext);

            MapReduceLauncher launcher = new MapReduceLauncher();
            boolean success = launcher.launchPig(plan, jobName, pigContext);
            if(success)
                return new HJob(ExecJob.JOB_STATUS.COMPLETED, pigContext, spec);
            else
                return new HJob(ExecJob.JOB_STATUS.FAILED, pigContext, null);

        } catch (Exception e) {
            // There are a lot of exceptions thrown by the launcher.  If this
            // is an ExecException, just let it through.  Else wrap it.
            if (e instanceof ExecException) throw (ExecException)e;
            else {
                int errCode = 2043;
                String msg = "Unexpected error during execution.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            }
        }

    }

    public ExecJob submit(PhysicalPlan plan,
                          String jobName) throws ExecException {
        throw new UnsupportedOperationException();
    }

    public void explain(PhysicalPlan plan, PrintStream stream) {
        try {
            PlanPrinter printer = new PlanPrinter(plan, stream);
            printer.visit();
            stream.println();

            ExecTools.checkLeafIsStore(plan, pigContext);

            MapReduceLauncher launcher = new MapReduceLauncher();
            launcher.explain(plan, pigContext, stream);
        } catch (Exception ve) {
            throw new RuntimeException(ve);
        }
    }

    public Collection<ExecJob> runningJobs(Properties properties) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public Collection<String> activeScopes() throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    public void reclaimScope(String scope) throws ExecException {
        throw new UnsupportedOperationException();
    }
    
    private void setSSHFactory(){
        Properties properties = this.pigContext.getProperties();
        String g = properties.getProperty("ssh.gateway");
        if (g == null || g.length() == 0) return;
        try {
            Class clazz = Class.forName("org.apache.pig.shock.SSHSocketImplFactory");
            SocketImplFactory f = (SocketImplFactory)clazz.getMethod("getFactory", new Class[0]).invoke(0, new Object[0]);
            Socket.setSocketImplFactory(f);
        } 
        catch (SocketException e) {}
        catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    //To prevent doing hod if the pig server is constructed multiple times
    private static String hodMapRed;
    private static String hodHDFS;
    private String hodConfDir = null; 
    private String remoteHodConfDir = null; 
    private Process hodProcess = null;

    class ShutdownThread extends Thread{
        public synchronized void run() {
            closeHod(pigContext.getProperties().getProperty("hod.server"));
        }
    }
    
    private String[] doHod(String server, Properties properties) throws ExecException {
        if (hodMapRed != null) {
            return new String[] {hodHDFS, hodMapRed};
        }
        
        try {
            // first, create temp director to store the configuration
            hodConfDir = createTempDir(server);
			
            //jz: fallback to systemproperty cause this not handled in Main
            hodParams = new StringBuilder(properties.getProperty(
                    "hod.param", System.getProperty("hod.param", "")));
            // get the number of nodes out of the command or use default
            int nodes = getNumNodes(hodParams);

            // command format: hod allocate - d <cluster_dir> -n <number_of_nodes> <other params>
            String[] fixedCmdArray = new String[] { "hod", "allocate", "-d",
                                       hodConfDir, "-n", Integer.toString(nodes) };
            String[] extraParams = hodParams.toString().split(" ");
    
            String[] cmdarray = new String[fixedCmdArray.length + extraParams.length];
            System.arraycopy(fixedCmdArray, 0, cmdarray, 0, fixedCmdArray.length);
            System.arraycopy(extraParams, 0, cmdarray, fixedCmdArray.length, extraParams.length);

            log.info("Connecting to HOD...");
            log.debug("sending HOD command " + cmdToString(cmdarray));

            // setup shutdown hook to make sure we tear down hod connection
            Runtime.getRuntime().addShutdownHook(new ShutdownThread());

            runCommand(server, cmdarray, true);

            // print all the information provided by HOD
            try {
                BufferedReader br = new BufferedReader(new InputStreamReader(hodProcess.getErrorStream()));
                String msg;
                while ((msg = br.readLine()) != null)
                    log.info(msg);
                br.close();
            } catch(IOException ioe) {}

            // for remote connection we need to bring the file locally  
            if (!server.equals(LOCAL))
                hodConfDir = copyHadoopConfLocally(server);

            String hdfs = null;
            String mapred = null;
            String hadoopConf = hodConfDir + "/hadoop-site.xml";

            log.info ("Hadoop configuration file: " + hadoopConf);

            JobConf jobConf = new JobConf(hadoopConf);
            jobConf.addResource("pig-cluster-hadoop-site.xml");

            //the method below alters the properties object by overriding the
            //hod properties with the values from properties and recomputing
            //the properties
            recomputeProperties(jobConf, properties);
            
            hdfs = properties.getProperty(FILE_SYSTEM_LOCATION);
            if (hdfs == null) {
                int errCode = 4007;
                String msg = "Missing fs.default.name from hadoop configuration.";
                throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
            }
            log.info("HDFS: " + hdfs);

            mapred = properties.getProperty(JOB_TRACKER_LOCATION);
            if (mapred == null) {
                int errCode = 4007;
                String msg = "Missing mapred.job.tracker from hadoop configuration";
                throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT);
            }
            log.info("JobTracker: " + mapred);

            // this is not longer needed as hadoop-site.xml given to us by HOD
            // contains data in the correct format
            // hdfs = fixUpDomain(hdfs, properties);
            // mapred = fixUpDomain(mapred, properties);
            hodHDFS = hdfs;
            hodMapRed = mapred;

            return new String[] {hdfs, mapred};
        } 
        catch (Exception e) {
            int errCode = 6010;
            String msg = "Could not connect to HOD";
            throw new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }
    }

    private synchronized void closeHod(String server){
            if (hodProcess == null){
                // just cleanup the dir if it exists and return
                if (hodConfDir != null)
                    deleteDir(server, hodConfDir);
                return;
            }

            // hod deallocate format: hod deallocate -d <conf dir>
            String[] cmdarray = new String[4];
			cmdarray[0] = "hod";
            cmdarray[1] = "deallocate";
            cmdarray[2] = "-d";
            if (remoteHodConfDir != null)
                cmdarray[3] = remoteHodConfDir;
            else
                cmdarray[3] = hodConfDir;
            
            log.info("Disconnecting from HOD...");
            log.debug("Disconnect command: " + cmdToString(cmdarray));

            try {
                runCommand(server, cmdarray, false);
           } catch (Exception e) {
                log.warn("Failed to disconnect from HOD; error: " + e.getMessage());
                hodProcess.destroy();
           } finally {
               if (remoteHodConfDir != null){
                   deleteDir(server, remoteHodConfDir);
                   if (hodConfDir != null)
                       deleteDir(LOCAL, hodConfDir);
               }else
                   deleteDir(server, hodConfDir);
           }

           hodProcess = null;
    }

    private String copyHadoopConfLocally(String server) throws ExecException {
        String localDir = createTempDir(LOCAL);
        String remoteFile = new String(hodConfDir + "/hadoop-site.xml");
        String localFile = new String(localDir + "/hadoop-site.xml");

        remoteHodConfDir = hodConfDir;

        String[] cmdarray = new String[2];
        cmdarray[0] = "cat";
        cmdarray[1] = remoteFile;

        Process p = runCommand(server, cmdarray, false);

        BufferedWriter bw;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(localFile)));
        } catch (Exception e){
            int errCode = 4008;
            String msg = "Failed to create local hadoop file " + localFile;
            throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT, e);
        }

        try {
            BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String line;
            while ((line = br.readLine()) != null){
                bw.write(line, 0, line.length());
                bw.newLine();
            }
            br.close();
            bw.close();
        } catch (Exception e){
            int errCode = 4009;
            String msg = "Failed to copy data to local hadoop file " + localFile;
            throw new ExecException(msg, errCode, PigException.USER_ENVIRONMENT, e);
        }

        return localDir;
    }

    private String cmdToString(String[] cmdarray) {
        StringBuilder cmd = new StringBuilder();

        for (int i = 0; i < cmdarray.length; i++) {
            cmd.append(cmdarray[i]);
            cmd.append(' ');
        }

        return cmd.toString();
    }
    private Process runCommand(String server, String[] cmdarray, boolean connect) throws ExecException {
        Process p;
        try {
            if (server.equals(LOCAL)) {
                p = Runtime.getRuntime().exec(cmdarray);
            } 
            else {
                SSHSocketImplFactory fac = SSHSocketImplFactory.getFactory(server);
                p = fac.ssh(cmdToString(cmdarray));
            }

            if (connect)
                hodProcess = p;

            //this should return as soon as connection is shutdown
            int rc = p.waitFor();
            if (rc != 0) {
                StringBuilder errMsg = new StringBuilder();
                try {
                    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
                    String line = null;
                    while((line = br.readLine()) != null) {
                        errMsg.append(line);
                    }
                    br.close();
                    br = new BufferedReader(new InputStreamReader(p.getErrorStream()));
                    line = null;
                    while((line = br.readLine()) != null) {
                        errMsg.append(line);
                    }
                    br.close();
                } catch (IOException ioe) {}
                int errCode = 6011;
                StringBuilder msg = new StringBuilder("Failed to run command ");
                msg.append(cmdToString(cmdarray));
                msg.append(" on server ");
                msg.append(server);
                msg.append("; return code: ");
                msg.append(rc);
                msg.append("; error: ");
                msg.append(errMsg.toString());
                throw new ExecException(msg.toString(), errCode, PigException.REMOTE_ENVIRONMENT);
            }
        } catch (Exception e){
            if(e instanceof ExecException) throw (ExecException)e;
            int errCode = 6012;
            String msg = "Unable to run command: " + cmdToString(cmdarray) + " on server " + server;
            throw new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, e);
        }

        return p;
    }

    /*
    private FileSpec checkLeafIsStore(PhysicalPlan plan) throws ExecException {
        try {
            PhysicalOperator leaf = (PhysicalOperator)plan.getLeaves().get(0);
            FileSpec spec = null;
            if(!(leaf instanceof POStore)){
                String scope = leaf.getOperatorKey().getScope();
                POStore str = new POStore(new OperatorKey(scope,
                    NodeIdGenerator.getGenerator().getNextNodeId(scope)));
                str.setPc(pigContext);
                spec = new FileSpec(FileLocalizer.getTemporaryPath(null,
                    pigContext).toString(),
                    new FuncSpec(BinStorage.class.getName()));
                str.setSFile(spec);
                plan.addAsLeaf(str);
            } else{
                spec = ((POStore)leaf).getSFile();
            }
            return spec;
        } catch (Exception e) {
            throw new ExecException(e);
        }
    }
    */

    private void deleteDir(String server, String dir) {
        if (server.equals(LOCAL)){
            File path = new File(dir);
            deleteLocalDir(path);
        }
        else { 
            // send rm command over ssh
            String[] cmdarray = new String[3];
			cmdarray[0] = "rm";
            cmdarray[1] = "-rf";
            cmdarray[2] = dir;

            try{
                runCommand(server, cmdarray, false);
            }catch(Exception e){
                    log.warn("Failed to remove HOD configuration directory - " + dir);
            }
        }
    }

    private void deleteLocalDir(File path){
        File[] files = path.listFiles();
        int i;
        for (i = 0; i < files.length; i++){
            if (files[i].isHidden())
                continue;
            if (files[i].isFile())
                files[i].delete();
            else if (files[i].isDirectory())
                deleteLocalDir(files[i]);
        }

        path.delete();
    }

    private String fixUpDomain(String hostPort,Properties properties) throws UnknownHostException {
        URI uri = null;
        try {
            uri = new URI(hostPort);
        } catch (URISyntaxException use) {
            throw new RuntimeException("Illegal hostPort: " + hostPort);
        }
        
        String hostname = uri.getHost();
        int port = uri.getPort();
        
        // Parse manually if hostPort wasn't non-opaque URI
        // e.g. hostPort is "myhost:myport"
        if (hostname == null || port == -1) {
            String parts[] = hostPort.split(":");
            hostname = parts[0];
            port = Integer.valueOf(parts[1]);
        }
        
        if (hostname.indexOf('.') == -1) {
          //jz: fallback to systemproperty cause this not handled in Main 
            String domain = properties.getProperty("cluster.domain",System.getProperty("cluster.domain"));
            if (domain == null) 
                throw new RuntimeException("Missing cluster.domain property!");
            hostname = hostname + "." + domain;
        }
        InetAddress.getByName(hostname);
        return hostname + ":" + Integer.toString(port);
    }

    // create temp dir to store hod output; removed on exit
    // format: <tempdir>/PigHod.<host name>.<user name>.<nanosecondts>
    private String createTempDir(String server) throws ExecException {
        StringBuilder tempDirPrefix  = new StringBuilder ();
        
        if (server.equals(LOCAL))
            tempDirPrefix.append(System.getProperty("java.io.tmpdir"));
        else
            // for remote access we assume /tmp as temp dir
            tempDirPrefix.append("/tmp");

        tempDirPrefix.append("/PigHod.");
        try {
            tempDirPrefix.append(InetAddress.getLocalHost().getHostName());
            tempDirPrefix.append(".");
        } catch (UnknownHostException e) {}
            
        tempDirPrefix.append(System.getProperty("user.name"));
        tempDirPrefix.append(".");
        String path;
        do {
            path = tempDirPrefix.toString() + System.nanoTime();
        } while (!createDir(server, path));

        return path;
    }

    private boolean createDir(String server, String dir) throws ExecException{
        if (server.equals(LOCAL)){ 
            // create local directory
            File tempDir = new File(dir);
            boolean success = tempDir.mkdir();
            if (!success)
                log.warn("Failed to create HOD configuration directory - " + dir + ". Retrying ...");

            return success;
        }
        else {
            String[] cmdarray = new String[2];
			cmdarray[0] = "mkdir ";
            cmdarray[1] = dir;

            try{
                runCommand(server, cmdarray, false);
            }
            catch(ExecException e){
                    log.warn("Failed to create HOD configuration directory - " + dir + "Retrying...");
                    return false;
            }

            return true;
        }
    }

    // returns number of nodes based on -m option in hodParams if present;
    // otherwise, default is used; -m is removed from the params
    int getNumNodes(StringBuilder hodParams) {
        String val = hodParams.toString();
        int startPos = val.indexOf("-m ");
        if (startPos == -1)
            startPos = val.indexOf("-m\t");
        if (startPos != -1) {
            int curPos = startPos + 3;
            int len = val.length();
            while (curPos < len && Character.isWhitespace(val.charAt(curPos))) curPos ++;
            int numStartPos = curPos;
            while (curPos < len && Character.isDigit(val.charAt(curPos))) curPos ++;
            int nodes = Integer.parseInt(val.substring(numStartPos, curPos));
            hodParams.delete(startPos, curPos);
            return nodes;
        } else {
            return Integer.getInteger("hod.nodes", 15);
        }
    }
    
    /**
     * Method to recompute pig properties by overriding hadoop properties
     * with pig properties
     * @param conf JobConf with appropriate hadoop resource files
     * @param properties Pig properties that will override hadoop properties; properties might be modified
     */
    private void recomputeProperties(JobConf jobConf, Properties properties) {
        // We need to load the properties from the hadoop configuration
        // We want to override these with any existing properties we have.
        if (jobConf != null && properties != null) {
            Properties hadoopProperties = new Properties();
            Iterator<Map.Entry<String, String>> iter = jobConf.iterator();
            while (iter.hasNext()) {
                Map.Entry<String, String> entry = iter.next();
                hadoopProperties.put(entry.getKey(), entry.getValue());
            }

            //override hadoop properties with user defined properties
            Enumeration<Object> propertiesIter = properties.keys();
            while (propertiesIter.hasMoreElements()) {
                String key = (String) propertiesIter.nextElement();
                String val = properties.getProperty(key);
                hadoopProperties.put(key, val);
            }
            
            //clear user defined properties and re-populate
            properties.clear();
            Enumeration<Object> hodPropertiesIter = hadoopProperties.keys();
            while (hodPropertiesIter.hasMoreElements()) {
                String key = (String) hodPropertiesIter.nextElement();
                String val = hadoopProperties.getProperty(key);
                properties.put(key, val);
            }

        }
    }
    
}





/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.builtin;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.ReversibleLoadStoreFunc;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataReaderWriter;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.io.BufferedPositionedInputStream;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.SchemaMergeException;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.impl.util.WrappedIOException;


public class BinStorage implements ReversibleLoadStoreFunc {
    public static final byte RECORD_1 = 0x01;
    public static final byte RECORD_2 = 0x02;
    public static final byte RECORD_3 = 0x03;

    Iterator<Tuple>     i              = null;
    protected BufferedPositionedInputStream in = null;
    private static final Log mLog = LogFactory.getLog(BinStorage.class);
    private DataInputStream inData = null;
    protected long                end            = Long.MAX_VALUE;
    
    /**
     * Simple binary nested reader format
     */
    public BinStorage() {
    }

    public Tuple getNext() throws IOException {
        
        byte b = 0;
//      skip to next record
        while (true) {
            if (in == null || in.getPosition() >=end) {
                return null;
            }
            // check if we saw RECORD_1 in our last attempt
            // this can happen if we have the following 
            // sequence RECORD_1-RECORD_1-RECORD_2-RECORD_3
            // After reading the second RECORD_1 in the above
            // sequence, we should not look for RECORD_1 again
            if(b != RECORD_1) {
                b = (byte) in.read();
                if(b != RECORD_1 && b != -1) {
                    continue;
                }
                if(b == -1) return null;
            }
            b = (byte) in.read();
            if(b != RECORD_2 && b != -1) {
                continue;
            }
            if(b == -1) return null;
            b = (byte) in.read();
            if(b != RECORD_3 && b != -1) {
                continue;
            }
            if(b == -1) return null;
            break;
        }
        try {
            return (Tuple)DataReaderWriter.readDatum(inData);
        } catch (ExecException ee) {
            throw ee;
        }
    }

    public void bindTo(String fileName, BufferedPositionedInputStream in, long offset, long end) throws IOException {
        this.in = in;
        inData = new DataInputStream(in);
        this.end = end;
    }


    DataOutputStream         out     = null;
  
    public void bindTo(OutputStream os) throws IOException {
        this.out = new DataOutputStream(new BufferedOutputStream(os));
    }

    public void finish() throws IOException {
        out.flush();
    }

    public void putNext(Tuple t) throws IOException {
        out.write(RECORD_1);
        out.write(RECORD_2);
        out.write(RECORD_3);
        t.write(out);
    }

    public DataBag bytesToBag(byte[] b){
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToBag(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to bag, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }        
    }

    public String bytesToCharArray(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToCharArray(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to chararray, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Double bytesToDouble(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Double(dis.readDouble());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to double, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Float bytesToFloat(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Float(dis.readFloat());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to float, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
            
            return null;
        }
    }

    public Integer bytesToInteger(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Integer(dis.readInt());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to integer, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Long bytesToLong(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return new Long(dis.readLong());
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to long, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Map<Object, Object> bytesToMap(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToMap(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to map, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    public Tuple bytesToTuple(byte[] b) {
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(b));
        try {
            return DataReaderWriter.bytesToTuple(dis);
        } catch (IOException e) {
            LogUtils.warn(this, "Unable to convert bytearray to tuple, " +
                    "caught IOException <" + e.getMessage() + ">",
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, 
                    mLog);
        
            return null;
        }
    }

    /* (non-Javadoc)
     * @see org.apache.pig.LoadFunc#determineSchema(java.lang.String, org.apache.pig.ExecType, org.apache.pig.backend.datastorage.DataStorage)
     */
    public Schema determineSchema(String fileName, ExecType execType,
            DataStorage storage) throws IOException {
        InputStream is = FileLocalizer.open(fileName, execType, storage);
        bindTo(fileName, new BufferedPositionedInputStream(is), 0, Long.MAX_VALUE);
        // get the first record from the input file
        // and figure out the schema from the data in
        // the first record
        Tuple t = getNext();
        is.close();
        if(t == null) {
            // we couldn't get a valid record from the input
            return null;
        }
        int numFields = t.size();
        Schema s = new Schema();
        for (int i = 0; i < numFields; i++) {
            try {
                s.add(DataType.determineFieldSchema(t.get(i)));
            } catch (Exception e) {
                int errCode = 2104;
                String msg = "Error while determining schema of BinStorage data.";
                throw new ExecException(msg, errCode, PigException.BUG, e);
            } 
        }
        return s;
    }

    public void fieldsToRead(Schema schema) {
        // TODO Auto-generated method stub
        
    }

    public byte[] toBytes(DataBag bag) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, bag);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting bag to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(String s) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, s);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting chararray to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Double d) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, d);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting double to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Float f) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, f);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting float to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Integer i) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, i);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting int to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Long l) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, l);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting long to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Map<Object, Object> m) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, m);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting map to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }

    public byte[] toBytes(Tuple t) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        try {
            DataReaderWriter.writeDatum(dos, t);
        } catch (Exception ee) {
            int errCode = 2105;
            String msg = "Error while converting tuple to bytes.";
            throw new ExecException(msg, errCode, PigException.BUG, ee);
        }
        return baos.toByteArray();
    }
    public boolean equals(Object obj) {
        return true;
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.tools.grunt;

import java.io.File;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.io.IOException;
import java.io.FileOutputStream;

import jline.ConsoleReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.GruntParser;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.tools.pigscript.parser.*;
import org.apache.pig.impl.logicalLayer.parser.TokenMgrError;
import org.apache.pig.impl.util.LogUtils;

public class Grunt 
{
    private final Log log = LogFactory.getLog(getClass());
    
    BufferedReader in;
    PigServer pig;
    GruntParser parser;    

    public Grunt(BufferedReader in, PigContext pigContext) throws ExecException
    {
        this.in = in;
        this.pig = new PigServer(pigContext);
        
        if (in != null)
        {
            parser = new GruntParser(in);
            parser.setParams(pig);    
        }
    }

    public void setConsoleReader(ConsoleReader c)
    {
        parser.setConsoleReader(c);
    }
    public void run() {        
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        while(true) {
            try {
                parser.setInteractive(true);
                parser.parseStopOnError();
                break;                            
            } catch(Throwable t) {
                LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), log, verbose);
                parser.ReInit(in);
            }
        }
    }

    public void exec() throws Throwable {
        boolean verbose = "true".equalsIgnoreCase(pig.getPigContext().getProperties().getProperty("verbose"));
        try {
            parser.setInteractive(false);
            parser.parseStopOnError();
        } catch (Throwable t) {
            LogUtils.writeLog(t, pig.getPigContext().getProperties().getProperty("pig.logfile"), log, verbose);
            throw (t);
        }
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig;

import java.io.*;
import java.util.*;
import java.util.jar.*;
import java.text.ParseException;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;
import jline.History;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.log4j.Level;
import org.apache.log4j.PropertyConfigurator;
import org.apache.pig.ExecType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.util.JarManager;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.tools.cmdline.CmdLineParser;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.grunt.PigCompletor;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.timer.PerformanceTimerFactory;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;

public class Main
{

    private final static Log log = LogFactory.getLog(Main.class);
    
    private static final String LOG4J_CONF = "log4jconf";
    private static final String BRIEF = "brief";
    private static final String DEBUG = "debug";
    private static final String JAR = "jar";
    private static final String VERBOSE = "verbose";
    
    private enum ExecMode {STRING, FILE, SHELL, UNKNOWN};
                
/**
 * The Main-Class for the Pig Jar that will provide a shell and setup a classpath appropriate
 * for executing Jar files.
 * 
 * @param args
 *            -jar can be used to add additional jar files (colon separated). - will start a
 *            shell. -e will execute the rest of the command line as if it was input to the
 *            shell.
 * @throws IOException
 */
public static void main(String args[])
{
    int rc = 1;
    Properties properties = new Properties();
    PropertiesUtil.loadPropertiesFromFile(properties);
    
    boolean verbose = false;
    boolean gruntCalled = false;
    String logFileName = null;

    try {
        BufferedReader pin = null;
        boolean debug = false;
        boolean dryrun = false;
        ArrayList<String> params = new ArrayList<String>();
        ArrayList<String> paramFiles = new ArrayList<String>();

        CmdLineParser opts = new CmdLineParser(args);
        opts.registerOpt('4', "log4jconf", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('b', "brief", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('c', "cluster", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('d', "debug", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('e', "execute", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('f', "file", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('h', "help", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('o', "hod", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('j', "jar", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('v', "verbose", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('x', "exectype", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('i', "version", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('p', "param", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('m', "param_file", CmdLineParser.ValueExpected.OPTIONAL);
        opts.registerOpt('r', "dryrun", CmdLineParser.ValueExpected.NOT_ACCEPTED);
        opts.registerOpt('l', "logfile", CmdLineParser.ValueExpected.REQUIRED);
        opts.registerOpt('w', "warning", CmdLineParser.ValueExpected.NOT_ACCEPTED);

        ExecMode mode = ExecMode.UNKNOWN;
        String file = null;
        ExecType execType = ExecType.MAPREDUCE ;
        String execTypeString = properties.getProperty("exectype");
        if(execTypeString!=null && execTypeString.length()>0){
            execType = PigServer.parseExecType(execTypeString);
        }
        String cluster = "local";
        String clusterConfigured = properties.getProperty("cluster");
        if(clusterConfigured != null && clusterConfigured.length() > 0){
            cluster = clusterConfigured;
        }
        
        //by default warning aggregation is on
        properties.setProperty("aggregate.warning", ""+true);

        char opt;
        while ((opt = opts.getNextOpt()) != CmdLineParser.EndOfOpts) {
            switch (opt) {
            case '4':
                String log4jconf = opts.getValStr();
                if(log4jconf != null){
                    properties.setProperty(LOG4J_CONF, log4jconf);
                }
                break;

            case 'b':
                properties.setProperty(BRIEF, "true");
                break;

            case 'c': 
                // Needed away to specify the cluster to run the MR job on
                // Bug 831708 - fixed
                String clusterParameter = opts.getValStr();
                if (clusterParameter != null && clusterParameter.length() > 0) {
                    cluster = clusterParameter;
                }
                break;

            case 'd':
                String logLevel = opts.getValStr();
                if (logLevel != null) {
                    properties.setProperty(DEBUG, logLevel);
                }
                debug = true;
                break;
                
            case 'e': 
                mode = ExecMode.STRING;
                break;

            case 'f':
                mode = ExecMode.FILE;
                file = opts.getValStr();
                break;

            case 'h':
                usage();
                return;

            case 'j': 
                String jarsString = opts.getValStr();
                if(jarsString != null){
                    properties.setProperty(JAR, jarsString);
                }
                break;

            case 'l':
                //call to method that validates the path to the log file 
                //and sets up the file to store the client side log file                
                String logFileParameter = opts.getValStr();
                if (logFileParameter != null && logFileParameter.length() > 0) {
                    logFileName = validateLogFile(logFileParameter, null);
                } else {
                    logFileName = validateLogFile(logFileName, null);
                }
                properties.setProperty("pig.logfile", logFileName);
                break;

            case 'm':
                paramFiles.add(opts.getValStr());
                break;
                            
            case 'o': 
                // TODO sgroschupf using system properties is always a very bad idea
                String gateway = System.getProperty("ssh.gateway");
                if (gateway == null || gateway.length() == 0) {
                    properties.setProperty("hod.server", "local");
                } else {
                    properties.setProperty("hod.server", System.getProperty("ssh.gateway"));
                }
                break;

            case 'p': 
                String val = opts.getValStr();
                params.add(opts.getValStr());
                break;
                            
            case 'r': 
                // currently only used for parameter substitution
                // will be extended in the future
                dryrun = true;
                break;
                            
            case 'v':
                properties.setProperty(VERBOSE, ""+true);
                verbose = true;
                break;

            case 'w':
                properties.setProperty("aggregate.warning", ""+false);
                break;

            case 'x':
                try {
                    execType = PigServer.parseExecType(opts.getValStr());
                    } catch (IOException e) {
                        throw new RuntimeException("ERROR: Unrecognized exectype.", e);
                    }
                break;
            case 'i':
            	System.out.println(getVersionString());
            	return;
            default: {
                Character cc = new Character(opt);
                throw new AssertionError("Unhandled option " + cc.toString());
                     }
            }
        }
        // configure logging
        configureLog4J(properties);
        // create the context with the parameter
        PigContext pigContext = new PigContext(execType, properties);
        
        if(logFileName == null) {
            logFileName = validateLogFile(null, null);
        }
        
        pigContext.getProperties().setProperty("pig.logfile", logFileName);

        LogicalPlanBuilder.classloader = pigContext.createCl(null);

        // construct the parameter substitution preprocessor
        Grunt grunt = null;
        BufferedReader in;
        String substFile = null;
        switch (mode) {
        case FILE:
            // Run, using the provided file as a pig file
            in = new BufferedReader(new FileReader(file));

            // run parameter substitution preprocessor first
            substFile = file + ".substituted";
            pin = runParamPreprocessor(in, params, paramFiles, substFile, debug || dryrun);
            if (dryrun){
                log.info("Dry run completed. Substituted pig script is at " + substFile);
                return;
            }

            logFileName = validateLogFile(logFileName, file);
            pigContext.getProperties().setProperty("pig.logfile", logFileName);

            // Set job name based on name of the script
            pigContext.getProperties().setProperty(PigContext.JOB_NAME, 
                                                   "PigLatin:" +new File(file).getName()
            );
            
            if (!debug)
                new File(substFile).deleteOnExit();
            
            grunt = new Grunt(pin, pigContext);
            gruntCalled = true;
            grunt.exec();
            rc = 0;
            return;

        case STRING: {
            // Gather up all the remaining arguments into a string and pass them into
            // grunt.
            StringBuffer sb = new StringBuffer();
            String remainders[] = opts.getRemainingArgs();
            for (int i = 0; i < remainders.length; i++) {
                if (i != 0) sb.append(' ');
                sb.append(remainders[i]);
            }
            in = new BufferedReader(new StringReader(sb.toString()));
            grunt = new Grunt(in, pigContext);
            gruntCalled = true;
            grunt.exec();
            rc = 0;
            return;
            }

        default:
            break;
        }

        // If we're here, we don't know yet what they want.  They may have just
        // given us a jar to execute, they might have given us a pig script to
        // execute, or they might have given us a dash (or nothing) which means to
        // run grunt interactive.
        String remainders[] = opts.getRemainingArgs();
        if (remainders == null) {
            // Interactive
            mode = ExecMode.SHELL;
            ConsoleReader reader = new ConsoleReader(System.in, new OutputStreamWriter(System.out));
            reader.addCompletor(new PigCompletor());
            reader.setDefaultPrompt("grunt> ");
            final String HISTORYFILE = ".pig_history";
            String historyFile = System.getProperty("user.home") + File.separator  + HISTORYFILE;
            reader.setHistory(new History(new File(historyFile)));
            ConsoleReaderInputStream inputStream = new ConsoleReaderInputStream(reader);
            grunt = new Grunt(new BufferedReader(new InputStreamReader(inputStream)), pigContext);
            grunt.setConsoleReader(reader);
            gruntCalled = true;
            grunt.run();
            rc = 0;
            return;
        } else {
            // They have a pig script they want us to run.
            if (remainders.length > 1) {
                   throw new RuntimeException("You can only run one pig script "
                    + "at a time from the command line.");
            }
            mode = ExecMode.FILE;
            in = new BufferedReader(new FileReader(remainders[0]));

            // run parameter substitution preprocessor first
            substFile = remainders[0] + ".substituted";
            pin = runParamPreprocessor(in, params, paramFiles, substFile, debug || dryrun);
            if (dryrun){
                log.info("Dry run completed. Substituted pig script is at " + substFile);
                return;
            }
            
            logFileName = validateLogFile(logFileName, remainders[0]);
            pigContext.getProperties().setProperty("pig.logfile", logFileName);

            if (!debug)
                new File(substFile).deleteOnExit();

            // Set job name based on name of the script
            pigContext.getProperties().setProperty(PigContext.JOB_NAME, 
                                                   "PigLatin:" +new File(remainders[0]).getName()
            );

            grunt = new Grunt(pin, pigContext);
            gruntCalled = true;
            grunt.exec();
            rc = 0;
            return;
        }

        // Per Utkarsh and Chris invocation of jar file via pig depricated.
    } catch (ParseException e) {
        usage();
        rc = 2;
    } catch (NumberFormatException e) {
        usage();
        rc = 2;
    } catch (PigException pe) {
        if(pe.retriable()) {
            rc = 1; 
        } else {
            rc = 2;
        }
        if(!gruntCalled) {
        	LogUtils.writeLog(pe, logFileName, log, verbose);
        }
    } catch (Throwable e) {
        rc = 2;
        if(!gruntCalled) {
        	LogUtils.writeLog(e, logFileName, log, verbose);
        }
    } finally {
        // clear temp files
        FileLocalizer.deleteTempFiles();
        PerformanceTimerFactory.getPerfTimerFactory().dumpTimers();
        System.exit(rc);
    }
}

//TODO jz: log4j.properties should be used instead
private static void configureLog4J(Properties properties) {
    // TODO Add a file appender for the logs
    // TODO Need to create a property in the properties file for it.
    // sgroschupf, 25Feb2008: this method will be obsolete with PIG-115.
     
    String log4jconf = properties.getProperty(LOG4J_CONF);
    String trueString = "true";
    boolean brief = trueString.equalsIgnoreCase(properties.getProperty(BRIEF));
    Level logLevel = Level.INFO;

    String logLevelString = properties.getProperty(DEBUG);
    if (logLevelString != null){
        logLevel = Level.toLevel(logLevelString, Level.INFO);
    }
    
    if (log4jconf != null) {
         PropertyConfigurator.configure(log4jconf);
     } else if (!brief ) {
         // non-brief logging - timestamps
         Properties props = new Properties();
         props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
         props.setProperty("log4j.appender.PIGCONSOLE",
                           "org.apache.log4j.ConsoleAppender");
         props.setProperty("log4j.appender.PIGCONSOLE.layout",
                           "org.apache.log4j.PatternLayout");
         props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern",
                           "%d [%t] %-5p %c - %m%n");
         props.setProperty("log4j.appender.PIGCONSOLE.target",
         "System.err");
         PropertyConfigurator.configure(props);
     } else {
         // brief logging - no timestamps
         Properties props = new Properties();
         props.setProperty("log4j.rootLogger", "INFO, PIGCONSOLE");
         props.setProperty("log4j.appender.PIGCONSOLE",
                           "org.apache.log4j.ConsoleAppender");
         props.setProperty("log4j.appender.PIGCONSOLE.layout",
                           "org.apache.log4j.PatternLayout");
         props.setProperty("log4j.appender.PIGCONSOLE.layout.ConversionPattern",
                           "%m%n");
         props.setProperty("log4j.appender.PIGCONSOLE.target",
         "System.err");
         PropertyConfigurator.configure(props);
     }
}
 
// returns the stream of final pig script to be passed to Grunt
private static BufferedReader runParamPreprocessor(BufferedReader origPigScript, ArrayList<String> params,
                                            ArrayList<String> paramFiles, String scriptFile, boolean createFile) 
                                throws org.apache.pig.tools.parameters.ParseException, IOException{
    ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
    String[] type1 = new String[1];
    String[] type2 = new String[1];

    if (createFile){
        BufferedWriter fw = new BufferedWriter(new FileWriter(scriptFile));
        psp.genSubstitutedFile (origPigScript, fw, params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles.toArray(type2) : null);
        return new BufferedReader(new FileReader (scriptFile));

    } else {
        StringWriter writer = new StringWriter();
        psp.genSubstitutedFile (origPigScript, writer,  params.size() > 0 ? params.toArray(type1) : null, 
                                paramFiles.size() > 0 ? paramFiles.toArray(type2) : null);
        return new BufferedReader(new StringReader(writer.toString()));
    }
}
    
private static String getVersionString() {
	String findContainingJar = JarManager.findContainingJar(Main.class);
	  try { 
		  StringBuffer buffer = new  StringBuffer();
          JarFile jar = new JarFile(findContainingJar); 
          final Manifest manifest = jar.getManifest(); 
          final Map <String,Attributes> attrs = manifest.getEntries(); 
          Attributes attr = attrs.get("org/apache/pig");
          String version = (String) attr.getValue("Implementation-Version");
          String svnRevision = (String) attr.getValue("Svn-Revision");
          String buildTime = (String) attr.getValue("Build-TimeStamp");
          // we use a version string similar to svn 
          //svn, version 1.4.4 (r25188)
          // compiled Sep 23 2007, 22:32:34
          return "Apache Pig version " + version + " (r" + svnRevision + ") \ncompiled "+buildTime;
      } catch (Exception e) { 
          throw new RuntimeException("unable to read pigs manifest file", e); 
      } 
}

public static void usage()
{
	System.out.println("\n"+getVersionString()+"\n");
    System.out.println("USAGE: Pig [options] [-] : Run interactively in grunt shell.");
    System.out.println("       Pig [options] -e[xecute] cmd [cmd ...] : Run cmd(s).");
    System.out.println("       Pig [options] [-f[ile]] file : Run cmds found in file.");
    System.out.println("  options include:");
    System.out.println("    -4, -log4jconf log4j configuration file, overrides log conf");
    System.out.println("    -b, -brief brief logging (no timestamps)");
    System.out.println("    -c, -cluster clustername, kryptonite is default");
    System.out.println("    -d, -debug debug level, INFO is default");
    System.out.println("    -h, -help display this message");
    System.out.println("    -j, -jar jarfile load jarfile"); 
    System.out.println("    -o, -hod read hod server from system property ssh.gateway");
    System.out.println("    -v, -verbose print all error messages to screen");
    System.out.println("    -x, -exectype local|mapreduce, mapreduce is default");
    System.out.println("    -i, -version display version information");
    System.out.println("    -l, -logfile path to client side log file; current working directory is default");
    System.out.println("    -w, -warning turn warning on; also turns warning aggregation off");
}

private static String validateLogFile(String logFileName, String scriptName) {
    String strippedDownScriptName = null;
    
    if(scriptName != null) {
        File scriptFile = new File(scriptName);
        if(!scriptFile.isDirectory()) {
            String scriptFileAbsPath;
            try {
                scriptFileAbsPath = scriptFile.getCanonicalPath();
            } catch (IOException ioe) {
                throw new AssertionError("Could not compute canonical path to the script file " + ioe.getMessage());      
            }            
            strippedDownScriptName = getFileFromCanonicalPath(scriptFileAbsPath);
        }
    }
    
    String defaultLogFileName = (strippedDownScriptName == null ? "pig_" : strippedDownScriptName) + new Date().getTime() + ".log";
    File logFile;    
    
    if(logFileName != null) {
        logFile = new File(logFileName);
    
        //Check if the file name is a directory 
        //append the default file name to the file
        if(logFile.isDirectory()) {            
            if(logFile.canWrite()) {
                try {
                    logFileName = logFile.getCanonicalPath() + File.separator + defaultLogFileName;
                } catch (IOException ioe) {
                    throw new AssertionError("Could not compute canonical path to the log file " + ioe.getMessage());       
                }
                return logFileName;
            } else {
                throw new AssertionError("Need write permission in the directory: " + logFileName + " to create log file.");
            }
        } else {
            //we have a relative path or an absolute path to the log file
            //check if we can write to the directory where this file is/will be stored
            
            if (logFile.exists()) {
                if(logFile.canWrite()) {
                    try {
                        logFileName = new File(logFileName).getCanonicalPath();
                    } catch (IOException ioe) {
                        throw new AssertionError("Could not compute canonical path to the log file " + ioe.getMessage());
                    }
                    return logFileName;
                } else {
                    //do not have write permissions for the log file
                    //bail out with an error message
                    throw new AssertionError("Cannot write to file: " + logFileName + ". Need write permission.");
                }
            } else {
                logFile = logFile.getParentFile();
                
                if(logFile != null) {
                    //if the directory is writable we are good to go
                    if(logFile.canWrite()) {
                        try {
                            logFileName = new File(logFileName).getCanonicalPath();
                        } catch (IOException ioe) {
                            throw new AssertionError("Could not compute canonical path to the log file " + ioe.getMessage());
                        }
                        return logFileName;
                    } else {
                        throw new AssertionError("Need write permission in the directory: " + logFile + " to create log file.");
                    }
                }//end if logFile != null else is the default in fall through                
            }//end else part of logFile.exists()
        }//end else part of logFile.isDirectory()
    }//end if logFileName != null
    
    //file name is null or its in the current working directory 
    //revert to the current working directory
    String currDir = System.getProperty("user.dir");
    logFile = new File(currDir);
    logFileName = currDir + File.separator + (logFileName == null? defaultLogFileName : logFileName);
    if(logFile.canWrite()) {        
        return logFileName;
    }    
    throw new RuntimeException("Cannot write to log file: " + logFileName);
}

private static String getFileFromCanonicalPath(String canonicalPath) {
    return canonicalPath.substring(canonicalPath.lastIndexOf(File.separator));
}

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.executionengine.ExecException;

/**
 * A class to handle reading and writing of intermediate results of data
 * types.  This class could also be used for storing permanent results.
 */
public class DataReaderWriter {
    private static TupleFactory mTupleFactory = TupleFactory.getInstance();
    private static BagFactory mBagFactory = BagFactory.getInstance();
    static final int UNSIGNED_SHORT_MAX = 65535;
    static final String UTF8 = "UTF-8";

    public static Tuple bytesToTuple(DataInput in) throws IOException {
        // Don't use Tuple.readFields, because it requires you to
        // create a tuple with no size and then append fields.
        // That's less efficient than allocating the tuple size up
        // front and then filling in the spaces.
        // Read the size.
        int sz = in.readInt();
        // if sz == 0, we construct an "empty" tuple -
        // presumably the writer wrote an empty tuple!
        if (sz < 0) {
            throw new IOException("Invalid size " + sz + " for a tuple");
        }
        Tuple t = mTupleFactory.newTuple(sz);
        for (int i = 0; i < sz; i++) {
            t.set(i, readDatum(in));
        }
        return t;

    }
    
    public static DataBag bytesToBag(DataInput in) throws IOException {
        DataBag bag = mBagFactory.newDefaultBag();
        bag.readFields(in);
        return bag;
    }
    
    public static Map<Object, Object> bytesToMap(DataInput in) throws IOException {
        int size = in.readInt();    
        Map<Object, Object> m = new HashMap<Object, Object>(size);
        for (int i = 0; i < size; i++) {
            Object key = readDatum(in);
            m.put(key, readDatum(in));
        }
        return m;    
    }
    
    public static String bytesToCharArray(DataInput in) throws IOException{
        int size = in.readUnsignedShort();
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new String(ba, DataReaderWriter.UTF8);
    }

    public static String bytesToBigCharArray(DataInput in) throws IOException{
        int size = in.readInt();
        byte[] ba = new byte[size];
        in.readFully(ba);
        return new String(ba, DataReaderWriter.UTF8);
    }
    
        
    public static Object readDatum(DataInput in) throws IOException, ExecException {
        // Read the data type
        byte b = in.readByte();
        switch (b) {
            case DataType.TUPLE: 
                return bytesToTuple(in);
            
            case DataType.BAG: 
                return bytesToBag(in);

            case DataType.MAP: 
                return bytesToMap(in);    

            case DataType.INTEGER:
                return new Integer(in.readInt());

            case DataType.LONG:
                return new Long(in.readLong());

            case DataType.FLOAT:
                return new Float(in.readFloat());

            case DataType.DOUBLE:
                return new Double(in.readDouble());

            case DataType.BOOLEAN:
                return new Boolean(in.readBoolean());

            case DataType.BYTE:
                return new Byte(in.readByte());

            case DataType.BYTEARRAY: {
                int size = in.readInt();
                byte[] ba = new byte[size];
                in.readFully(ba);
                return new DataByteArray(ba);
                                     }

            case DataType.BIGCHARARRAY: 
                return bytesToBigCharArray(in);
            

            case DataType.CHARARRAY: 
                return bytesToCharArray(in);
            
            case DataType.NULL:
                return null;

            default:
                throw new RuntimeException("Unexpected data type " + b +
                    " found in stream.");
        }
    }

    public static void writeDatum(
            DataOutput out,
            Object val) throws IOException {
        // Read the data type
        byte type = DataType.findType(val);
        switch (type) {
            case DataType.TUPLE:
                // Because tuples are written directly by hadoop, the
                // tuple's write method needs to write the indicator byte.
                // So don't write the indicator byte here as it is for
                // everyone else.
                ((Tuple)val).write(out);
                break;
                
            case DataType.BAG:
                out.writeByte(DataType.BAG);
                ((DataBag)val).write(out);
                break;

            case DataType.MAP: {
                out.writeByte(DataType.MAP);
                Map<Object, Object> m = (Map<Object, Object>)val;
                out.writeInt(m.size());
                Iterator<Map.Entry<Object, Object> > i =
                    m.entrySet().iterator();
                while (i.hasNext()) {
                    Map.Entry<Object, Object> entry = i.next();
                    writeDatum(out, entry.getKey());
                    writeDatum(out, entry.getValue());
                }
                break;
                               }

            case DataType.INTEGER:
                out.writeByte(DataType.INTEGER);
                out.writeInt((Integer)val);
                break;

            case DataType.LONG:
                out.writeByte(DataType.LONG);
                out.writeLong((Long)val);
                break;

            case DataType.FLOAT:
                out.writeByte(DataType.FLOAT);
                out.writeFloat((Float)val);
                break;

            case DataType.DOUBLE:
                out.writeByte(DataType.DOUBLE);
                out.writeDouble((Double)val);
                break;

            case DataType.BOOLEAN:
                out.writeByte(DataType.BOOLEAN);
                out.writeBoolean((Boolean)val);
                break;

            case DataType.BYTE:
                out.writeByte(DataType.BYTE);
                out.writeByte((Byte)val);
                break;

            case DataType.BYTEARRAY: {
                out.writeByte(DataType.BYTEARRAY);
                DataByteArray bytes = (DataByteArray)val;
                out.writeInt(bytes.size());
                out.write(bytes.mData);
                break;
                                     }

            case DataType.CHARARRAY: {
                String s = (String)val;
                byte[] utfBytes = s.getBytes(DataReaderWriter.UTF8);
                int length = utfBytes.length;
                
                if(length < DataReaderWriter.UNSIGNED_SHORT_MAX) {
                    out.writeByte(DataType.CHARARRAY);
                    out.writeShort(length);
                    out.write(utfBytes);
                } else {
                	out.writeByte(DataType.BIGCHARARRAY);
                	out.writeInt(length);
                	out.write(utfBytes);
                }
                break;
                                     }

            case DataType.NULL:
                out.writeByte(DataType.NULL);
                break;

            default:
                throw new RuntimeException("Unexpected data type " + type +
                    " found in stream.");
        }
    }
}


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.MRCompiler.LastInputStreamingOptimizer;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRPrinter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MRStreamHandler;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.POPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POJoinPackage;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.plan.CompilationMessageCollector.Message;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.util.ConfigurationValidator;

/**
 * Main class that launches pig for Map Reduce
 *
 */
public class MapReduceLauncher extends Launcher{
    private static final Log log = LogFactory.getLog(MapReduceLauncher.class);
 
    //used to track the exception thrown by the job control which is run in a separate thread
    private Exception jobControlException = null;
    private boolean aggregateWarning = false;
    
    @Override
    public boolean launchPig(PhysicalPlan php,
                             String grpName,
                             PigContext pc) throws PlanException,
                                                   VisitorException,
                                                   IOException,
                                                   ExecException,
                                                   JobCreationException,
                                                   Exception {
        long sleepTime = 5000;
        aggregateWarning = "true".equalsIgnoreCase(pc.getProperties().getProperty("aggregate.warning"));
        MROperPlan mrp = compile(php, pc);
        
        ExecutionEngine exe = pc.getExecutionEngine();
        ConfigurationValidator.validatePigProperties(exe.getConfiguration());
        Configuration conf = ConfigurationUtil.toConfiguration(exe.getConfiguration());
        JobClient jobClient = ((HExecutionEngine)exe).getJobClient();

        JobControlCompiler jcc = new JobControlCompiler();
        
        JobControl jc = jcc.compile(mrp, grpName, conf, pc);
        
        int numMRJobs = jc.getWaitingJobs().size();
        
        //create the exception handler for the job control thread
        //and register the handler with the job control thread
        JobControlThreadExceptionHandler jctExceptionHandler = new JobControlThreadExceptionHandler();
        Thread jcThread = new Thread(jc);
        jcThread.setUncaughtExceptionHandler(jctExceptionHandler);
        jcThread.start();

        double lastProg = -1;
        int perCom = 0;
        while(!jc.allFinished()){
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {}
            double prog = calculateProgress(jc, jobClient)/numMRJobs;
            if(prog>=(lastProg+0.01)){
                perCom = (int)(prog * 100);
                if(perCom!=100)
                    log.info( perCom + "% complete");
            }
            lastProg = prog;
        }
        
        //check for the jobControlException first
        //if the job controller fails before launching the jobs then there are
        //no jobs to check for failure
        if(jobControlException != null) {
        	if(jobControlException instanceof PigException) {
        		throw jobControlException;
        	} else {
	        	int errCode = 2117;
	        	String msg = "Unexpected error when launching map reduce job.";        	
	    		throw new ExecException(msg, errCode, PigException.BUG, jobControlException);
        	}
        }
        
        // Look to see if any jobs failed.  If so, we need to report that.
        List<Job> failedJobs = jc.getFailedJobs();
        if (failedJobs != null && failedJobs.size() > 0) {
            log.error("Map reduce job failed");
            for (Job fj : failedJobs) {
                getStats(fj, jobClient, true, pc);
            }
            jc.stop(); 
            return false;
        }

        Map<Enum, Long> warningAggMap = new HashMap<Enum, Long>();
                
        List<Job> succJobs = jc.getSuccessfulJobs();
        if(succJobs!=null)
            for(Job job : succJobs){
                getStats(job,jobClient, false, pc);
                if(aggregateWarning) {
                	computeWarningAggregate(job, jobClient, warningAggMap);
                }
            }

        jc.stop();
        
        if(aggregateWarning) {
        	CompilationMessageCollector.logAggregate(warningAggMap, MessageType.Warning, log) ;
        }

        log.info( "100% complete");
        log.info("Success!");
        return true;
    }

    @Override
    public void explain(
            PhysicalPlan php,
            PigContext pc,
            PrintStream ps) throws PlanException, VisitorException,
                                   IOException {
        log.trace("Entering MapReduceLauncher.explain");
        MROperPlan mrp = compile(php, pc);

        MRPrinter printer = new MRPrinter(ps, mrp);
        printer.visit();
    }

    private MROperPlan compile(
            PhysicalPlan php,
            PigContext pc) throws PlanException, IOException, VisitorException {
        MRCompiler comp = new MRCompiler(php, pc);
        comp.randomizeFileLocalizer();
        comp.compile();
        MROperPlan plan = comp.getMRPlan();
        
        //display the warning message(s) from the MRCompiler
        comp.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        
        String lastInputChunkSize = 
            pc.getProperties().getProperty(
                    "last.input.chunksize", POJoinPackage.DEFAULT_CHUNK_SIZE);
        String prop = System.getProperty("pig.exec.nocombiner");
        if (!("true".equals(prop)))  {
            CombinerOptimizer co = new CombinerOptimizer(plan, lastInputChunkSize);
            co.visit();
            //display the warning message(s) from the CombinerOptimizer
            co.getMessageCollector().logMessages(MessageType.Warning, aggregateWarning, log);
        }
        
        // optimize key - value handling in package
        POPackageAnnotator pkgAnnotator = new POPackageAnnotator(plan);
        pkgAnnotator.visit();
        
        // check whether stream operator is present
        MRStreamHandler checker = new MRStreamHandler(plan);
        checker.visit();
        
        // optimize joins
        LastInputStreamingOptimizer liso = 
            new MRCompiler.LastInputStreamingOptimizer(plan, lastInputChunkSize);
        liso.visit();
        
        // figure out the type of the key for the map plan
        // this is needed when the key is null to create
        // an appropriate NullableXXXWritable object
        KeyTypeDiscoveryVisitor kdv = new KeyTypeDiscoveryVisitor(plan);
        kdv.visit();
        return plan;
    }
    
    /**
     * An exception handler class to handle exceptions thrown by the job controller thread
     * Its a local class. This is the only mechanism to catch unhandled thread exceptions
     * Unhandled exceptions in threads are handled by the VM if the handler is not registered
     * explicitly or if the default handler is null
     */
    class JobControlThreadExceptionHandler implements Thread.UncaughtExceptionHandler {
    	
    	public void uncaughtException(Thread thread, Throwable throwable) {
    		ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		PrintStream ps = new PrintStream(baos);
    		throwable.printStackTrace(ps);
    		String exceptionString = baos.toString();    		
    		try {	
    			jobControlException = getExceptionFromString(exceptionString);
    		} catch (Exception e) {
    			String errMsg = "Could not resolve error that occured when launching map reduce job.";
    			jobControlException = new RuntimeException(errMsg, e);
    		}
    	}
    }
    
    void computeWarningAggregate(Job job, JobClient jobClient, Map<Enum, Long> aggMap) {
    	JobID mapRedJobID = job.getAssignedJobID();
    	RunningJob runningJob = null;
    	try {
    		runningJob = jobClient.getJob(mapRedJobID);
    		if(runningJob != null) {
    		Counters counters = runningJob.getCounters();
        		for(Enum e : PigWarning.values()) {
        			Long currentCount = aggMap.get(e);
        			currentCount = (currentCount == null? 0 : currentCount);
        			currentCount += counters.getCounter(e);
        			aggMap.put(e, currentCount);
        		}
    		}
    	} catch (IOException ioe) {
    		String msg = "Unable to retrieve job to compute warning aggregation.";
    		log.warn(msg);
    	}    	
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.ExecType;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.LogUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.org.apache.bcel.internal.ExceptionConstants;
import com.sun.org.apache.xerces.internal.impl.xpath.regex.ParseException;

import junit.framework.TestCase;

public class TestBestFitCast extends TestCase {
    private PigServer pigServer;
    private MiniCluster cluster = MiniCluster.buildCluster();
    private File tmpFile, tmpFile2;
    int LOOP_SIZE = 20;
    
    public TestBestFitCast() throws ExecException, IOException{
        pigServer = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
//        pigServer = new PigServer(ExecType.LOCAL);
        tmpFile = File.createTempFile("test", "txt");
        PrintStream ps = new PrintStream(new FileOutputStream(tmpFile));
        long l = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(l + "\t" + i);
        }
        ps.close();
        
        tmpFile2 = File.createTempFile("test2", "txt");
        ps = new PrintStream(new FileOutputStream(tmpFile2));
        l = 0;
        for(int i = 1; i <= LOOP_SIZE; i++) {
            ps.println(l + "\t" + i + "\t" + i);
        }
        ps.close();
    }
    
    @Before
    public void setUp() throws Exception {
        
    }

    @After
    public void tearDown() throws Exception {
    }
    
    public static class UDF1 extends EvalFunc<Tuple>{
        /**
         * java level API
         * @param input expects a single numeric DataAtom value
         * @param output returns a single numeric DataAtom value, cosine value of the argument
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }

        /* (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),new Schema.FieldSchema(null, DataType.FLOAT)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.LONG),new Schema.FieldSchema(null, DataType.DOUBLE)))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.FLOAT))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.DOUBLE))));
            /*funcList.add(new FuncSpec(DoubleMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.DOUBLE)));
            funcList.add(new FuncSpec(FloatMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.FLOAT)));
            funcList.add(new FuncSpec(IntMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.INTEGER)));
            funcList.add(new FuncSpec(LongMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.LONG)));
            funcList.add(new FuncSpec(StringMax.class.getName(), Schema.generateNestedSchema(DataType.BAG, DataType.CHARARRAY)));*/
            return funcList;
        }    

    }
    
    public static class UDF2 extends EvalFunc<String>{
        /**
         * java level API
         * @param input expects a single numeric DataAtom value
         * @param output returns a single numeric DataAtom value, cosine value of the argument
         */
        @Override
        public String exec(Tuple input) throws IOException {
            try{
                String str = (String)input.get(0);
                return str.toUpperCase();
            }catch (Exception e){
                return null;
            }
        }

        /* (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            funcList.add(new FuncSpec(this.getClass().getName(), new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY))));
            return funcList;
        }    

    }
    
    /**
     * For testing with input schemas which have byte arrays
     */
    public static class UDF3 extends EvalFunc<Tuple>{
        
        /**
         * a UDF which simply returns its input as output
         */
        @Override
        public Tuple exec(Tuple input) throws IOException {
            return input;
        }

        /* (non-Javadoc)
         * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
         */
        @Override
        public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
            List<FuncSpec> funcList = new ArrayList<FuncSpec>();
            
            // the following schema should match when the input is
            // just a {bytearray} - exact match
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(new Schema.FieldSchema(null, DataType.BYTEARRAY))));
            // the following schema should match when the input is
            // just a {int} - exact match
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(new Schema.FieldSchema(null, DataType.INTEGER))));
            
            // The following two schemas will cause conflict when input schema
            // is {float, bytearray} since bytearray can be casted either to long
            // or double. However when input schema is {bytearray, int}, it should work
            // since bytearray should get casted to float and int to long. Likewise if
            // input schema is {bytearray, long} or {bytearray, double} it should work
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.DOUBLE)))));
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.LONG)))));
            
            
            // The following two schemas will cause conflict when input schema is
            // {bytearray, int, int} since the two ints could be casted to long, double
            // or double, long. Likewise input schema of either {bytearray, long, long}
            // or {bytearray, double, double} would cause conflict. Input schema of
            // {bytearray, long, double} or {bytearray, double, long} should not cause
            // conflict since only the bytearray needs to be casted to float. Input schema
            // of {float, bytearray, long} or {float, long, bytearray} should also
            // work since only the bytearray needs to be casted. Input schema of
            // {float, bytearray, int} will cause conflict since we could cast int to 
            // long or double and bytearray to long or double. Input schema of
            // {bytearray, long, int} should work and should match the first schema below for 
            // matching wherein the bytearray is cast to float and the int to double.
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.DOUBLE),
                            new Schema.FieldSchema(null, DataType.LONG)))));
            funcList.add(new FuncSpec(this.getClass().getName(), 
                    new Schema(Arrays.asList(new Schema.FieldSchema(null, DataType.FLOAT),
                            new Schema.FieldSchema(null, DataType.LONG),
                            new Schema.FieldSchema(null, DataType.DOUBLE)))));
            
            return funcList;
        }    

    }

    @Test
    public void testByteArrayCast1() throws IOException {
        //Passing (float, bytearray)
        //Ambiguous matches: (float, long) , (float, double)
        boolean exceptionCaused = false;
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:float, y);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
        try {
            Iterator<Tuple> iter = pigServer.openIterator("B");
        } catch(Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("{float,double}, {float,long}"));
        }
        assertTrue(exceptionCaused);
    }
    
    @Test
    public void testByteArrayCast2() throws IOException, ExecException {
        // Passing (bytearray, int)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, long) since in both cases bytearray is cast to float and the
        // cost of casting int to long < int to double
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast3() throws IOException, ExecException {
        // Passing (bytearray, long)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x, y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast4() throws IOException, ExecException {
        // Passing (bytearray, double)
        // Possible matches: (float, long) , (float, double)
        // Chooses (float, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:double);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }

    @Test
    public void testByteArrayCast5() throws IOException, ExecException {
        // Passing (bytearray, int, int )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two ints cannot be unambiguously
        // casted
        boolean exceptionCaused = false;
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y, y);");
        try {
            Iterator<Tuple> iter = pigServer.openIterator("B");
        }catch(Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double})"));
        }
        assertTrue(exceptionCaused);
    }
    
    @Test
    public void testByteArrayCast6() throws IOException, ExecException {
        // Passing (bytearray, long, long )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two longs cannot be
        // unambiguously casted
        boolean exceptionCaused = false;
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y, y);");
        try {
            Iterator<Tuple> iter = pigServer.openIterator("B");
        }catch(Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double})"));
        }
        assertTrue(exceptionCaused);
    }
    
    @Test
    public void testByteArrayCast7() throws IOException, ExecException {
        // Passing (bytearray, double, double )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // bytearray can be casted to float but the two doubles cannot be 
        // casted with a permissible cast
        boolean exceptionCaused = false;
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:double);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y, y);");
        try {
            Iterator<Tuple> iter = pigServer.openIterator("B");
        }catch(Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Could not infer the matching function"));
        }
        assertTrue(exceptionCaused);
    }
    
    @Test
    public void testByteArrayCast8() throws IOException, ExecException {
        // Passing (bytearray, long, double)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x, y:long, z:double);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast9() throws IOException, ExecException {
        // Passing (bytearray, double, long)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, double, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x, y:double, z:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(2), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast10() throws IOException, ExecException {
        // Passing (float, long, bytearray)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x:float, y:long, z);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast11() throws IOException, ExecException {
        // Passing (float, bytearray, long)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, double, long) since that is the only exact match without bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x:float, y, z:long);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(1), new Double(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(2), new Long(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast12() throws IOException, ExecException {
        // Passing (float, bytearray, int )
        // Ambiguous matches: (float, long, double) , (float, double, long)
        // will cause conflict since we could cast int to 
        // long or double and bytearray to long or double.
        boolean exceptionCaused = false;
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x:float, y, z:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y, y);");
        try {
            Iterator<Tuple> iter = pigServer.openIterator("B");
        }catch(Exception e) {
            exceptionCaused = true;
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Multiple matching functions"));
            assertTrue(msg.contains("({float,double,long}, {float,long,double}"));
        }
        assertTrue(exceptionCaused);
    }
    
    @Test
    public void testByteArrayCast13() throws IOException, ExecException {
        // Passing (bytearray, long, int)
        // Possible matches: (float, long, double) , (float, double, long)
        // Chooses (float, long, double) since for the bytearray there is a 
        // single unambiguous cast to float. For the other two args, it is
        // less "costlier" to cast the last int to double than cast the long
        // to double and int to long
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile2.toString()) + "' as (x, y:long, z:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF3.class.getName() + "(x,y,z);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals((Float)((Tuple)t.get(1)).get(0), 0.0f);
            assertTrue(((Tuple)t.get(1)).get(1) instanceof Long);
            assertEquals((Long)((Tuple)t.get(1)).get(1), new Long(cnt + 1));
            assertTrue(((Tuple)t.get(1)).get(2) instanceof Double);
            assertEquals((Double)((Tuple)t.get(1)).get(2), new Double(cnt + 1));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast14() throws IOException, ExecException {
        // Passing (bag{(bytearray)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(bytearray)} because it is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }
    
    @Test
    public void testByteArrayCast15() throws IOException, ExecException {
        // Passing (bytearray)
        // Possible matches: (bytearray), (int)
        // Chooses (bytearray) because that is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y);");
        pigServer.registerQuery("B = FOREACH A generate " + UDF3.class.getName() + "(y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(0)).get(0) instanceof DataByteArray);
            byte[] expected = Integer.toString(cnt + 1).getBytes();
            byte[] actual = ((DataByteArray)((Tuple)t.get(0)).get(0)).get();
            assertEquals(expected.length, actual.length);
            for(int i = 0; i < expected.length; i++) {
                assertEquals(expected[i], actual[i]);
            }
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testByteArrayCast16() throws IOException, ExecException {
        // Passing (int)
        // Possible matches: (bytearray), (int)
        // Chooses (int) because that is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:int);");
        pigServer.registerQuery("B = FOREACH A generate " + UDF3.class.getName() + "(y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertTrue(((Tuple)t.get(0)).get(0) instanceof Integer);
            assertEquals(new Integer(cnt + 1), (Integer)((Tuple)t.get(0)).get(0));
            ++cnt;
        }
        assertEquals(LOOP_SIZE, cnt);
    }
    
    @Test
    public void testIntSum() throws IOException, ExecException {
        // Passing (bag{(int)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(int)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:int);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Long);
        assertEquals(new Long(210), (Long)t.get(0));
    }
    
    @Test
    public void testLongSum() throws IOException, ExecException {
        // Passing (bag{(long)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(long)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:long);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Long);
        assertEquals(new Long(210), (Long)t.get(0));
    }
    
    @Test
    public void testFloatSum() throws IOException, ExecException {
        // Passing (bag{(float)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(float)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:float);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }
    
    @Test
    public void testDoubleSum() throws IOException, ExecException {
        // Passing (bag{(double)})
        // Possible matches: bag{(bytearray)}, bag{(int)}, bag{(long)}, bag{(float)}, bag{(double)}
        // Chooses bag{(double)} since it is an exact match
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x, y:double);");
        pigServer.registerQuery("B = group A all;");
        pigServer.registerQuery("C = FOREACH B generate SUM(A.y);");
        Iterator<Tuple> iter = pigServer.openIterator("C");
        Tuple t = iter.next();
        assertTrue(t.get(0) instanceof Double);
        assertEquals(new Double(210), (Double)t.get(0));
    }
    
    @Test
    public void test1() throws Exception{
        //Passing (long, int)
        //Possible matches: (float, float) , (long, double)
        //Chooses (long, double) as it has only one cast compared to two for (float, float)
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x,y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Long);
            assertEquals(true,((Tuple)t.get(1)).get(1) instanceof Double);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test2() throws Exception{
        //Passing (int, int)
        //Possible matches: (float, float) , (long, double)
        //Throws Exception as ambiguous definitions found
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(y,y);");
        try{
            pigServer.openIterator("B");
        }catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertEquals(true,msg.contains("as multiple or none of them fit"));
        }
        
    }
    
    @Test
    public void test3() throws Exception{
        //Passing (int, int)
        //Possible matches: (float, float) , (long, double)
        //Chooses (float, float) as both options lead to same score and (float, float) occurs first.
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "((float)y,(float)y);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Float);
            assertEquals(true,((Tuple)t.get(1)).get(1) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test4() throws Exception{
        //Passing (long)
        //Possible matches: (float), (integer), (double)
        //Chooses (float) as it leads to a better score that to (double)
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "' as (x:long, y:int);");
        pigServer.registerQuery("B = FOREACH A generate x, " + UDF1.class.getName() + "(x);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        int cnt = 0;
        while(iter.hasNext()){
            Tuple t = iter.next();
            assertEquals(true,((Tuple)t.get(1)).get(0) instanceof Float);
            ++cnt;
        }
        assertEquals(20, cnt);
    }
    
    @Test
    public void test5() throws Exception{
        //Passing bytearrays
        //Possible matches: (float, float) , (long, double)
        //Throws exception since more than one funcSpec and inp is bytearray
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(tmpFile.toString()) + "';");
        pigServer.registerQuery("B = FOREACH A generate $0, " + UDF1.class.getName() + "($1,$1);");
        try{
            pigServer.openIterator("B");
        }catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertEquals(true,msg.contains("Multiple matching functions"));
        }
        
    }

    @Test
    public void test6() throws Exception{
        // test UDF with single mapping function 
        // where bytearray is passed in as input parameter
        File input = Util.createInputFile("tmp", "", new String[] {"abc"});
        pigServer.registerQuery("A = LOAD '" + Util.generateURI(input.toString()) +"';");
        pigServer.registerQuery("B = FOREACH A GENERATE " + UDF2.class.getName() + "($0);");
        Iterator<Tuple> iter = pigServer.openIterator("B");
        if(!iter.hasNext()) fail("No Output received");
        Tuple t = iter.next();
        assertEquals("ABC", t.get(0));
    }
}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecJob;
import org.apache.pig.backend.executionengine.ExecJob.JOB_STATUS;
import org.apache.pig.builtin.BinStorage;
import org.apache.pig.builtin.PigStorage;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.io.FileLocalizer;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.LogicalOperator;
import org.apache.pig.impl.logicalLayer.LogicalPlan;
import org.apache.pig.impl.logicalLayer.LogicalPlanBuilder;
import org.apache.pig.impl.logicalLayer.LOPrinter;
import org.apache.pig.impl.logicalLayer.PlanSetter;
import org.apache.pig.impl.logicalLayer.optimizer.LogicalOptimizer;
import org.apache.pig.impl.logicalLayer.parser.ParseException;
import org.apache.pig.impl.logicalLayer.parser.QueryParser;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.validators.LogicalPlanValidationExecutor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.CompilationMessageCollector;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.CompilationMessageCollector.MessageType;
import org.apache.pig.impl.streaming.StreamingCommand;
import org.apache.pig.impl.util.PropertiesUtil;
import org.apache.pig.impl.logicalLayer.LODefine;
import org.apache.pig.impl.logicalLayer.LOStore;
import org.apache.pig.pen.ExampleGenerator;
import org.apache.pig.impl.util.LogUtils;
import org.apache.pig.tools.grunt.GruntParser;


/**
 * 
 * This class is the program's connection to Pig. Typically a program will create a PigServer
 * instance. The programmer then registers queries using registerQuery() and
 * retrieves results using openIterator() or store().
 * 
 */
public class PigServer {
    
    private final Log log = LogFactory.getLog(getClass());
    
    public static ExecType parseExecType(String str) throws IOException {
        String normStr = str.toLowerCase();
        
        if (normStr.equals("local")) return ExecType.LOCAL;
        if (normStr.equals("mapreduce")) return ExecType.MAPREDUCE;
        if (normStr.equals("mapred")) return ExecType.MAPREDUCE;
        if (normStr.equals("pig")) return ExecType.PIG;
        if (normStr.equals("pigbody")) return ExecType.PIG;
   
        int errCode = 2040;
        String msg = "Unknown exec type: " + str;
        throw new PigException(msg, errCode, PigException.BUG);
    }


    Map<LogicalOperator, LogicalPlan> aliases = new HashMap<LogicalOperator, LogicalPlan>();
    Map<OperatorKey, LogicalOperator> opTable = new HashMap<OperatorKey, LogicalOperator>();
    Map<String, LogicalOperator> aliasOp = new HashMap<String, LogicalOperator>();
    PigContext pigContext;
    
    private String scope = constructScope();
    private ArrayList<String> cachedScript = new ArrayList<String>();
    private boolean aggregateWarning = true;
    
    private String constructScope() {
        // scope servers for now as a session id
        // scope = user_id + "-" + time_stamp;
        
        String user = System.getProperty("user.name", "DEFAULT_USER_ID");
        String date = (new Date()).toString();
       
        return user + "-" + date;
    }
    
    public PigServer(String execTypeString) throws ExecException, IOException {
        this(parseExecType(execTypeString));
    }
    
    public PigServer(ExecType execType) throws ExecException {
        this(execType, PropertiesUtil.loadPropertiesFromFile());
    }

    public PigServer(ExecType execType, Properties properties) throws ExecException {
        this(new PigContext(execType, properties), true);
    }
  
    public PigServer(PigContext context) throws ExecException {
        this(context, true);
    }
    
    public PigServer(PigContext context, boolean connect) throws ExecException {
        this.pigContext = context;
        if (this.pigContext.getProperties().getProperty(PigContext.JOB_NAME) == null) {
            setJobName("DefaultJobName") ;
        }
        
        aggregateWarning = "true".equalsIgnoreCase(pigContext.getProperties().getProperty("aggregate.warning"));
        
        if (connect) {
            pigContext.connect();
        }
    }

    public PigContext getPigContext(){
        return pigContext;
    }
    
    public void debugOn() {
        pigContext.debug = true;
    }
    
    public void debugOff() {
        pigContext.debug = false;
    }
    
    /**
     * Add a path to be skipped while automatically shipping binaries for 
     * streaming.
     *  
     * @param path path to be skipped
     */
    public void addPathToSkip(String path) {
        pigContext.addPathToSkip(path);
    }
    
    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the 
     * constructor.
     * 
     * @param function - the new function alias to define.
     * @param functionSpec - the name of the function and any arguments.
     * It should have the form: classname('arg1', 'arg2', ...)
     */
    @Deprecated
    public void registerFunction(String function, String functionSpec) {
        registerFunction(function, new FuncSpec(functionSpec));
    }
    
    /**
     * Defines an alias for the given function spec. This
     * is useful for functions that require arguments to the 
     * constructor.
     * 
     * @param function - the new function alias to define.
     * @param funcSpec - the FuncSpec object representing the name of 
     * the function class and any arguments to constructor.
     */
    public void registerFunction(String function, FuncSpec funcSpec) {
        pigContext.registerFunction(function, funcSpec);
    }
    
    /**
     * Defines an alias for the given streaming command.
     * 
     * @param commandAlias - the new command alias to define
     * @param command - streaming command to be executed
     */
    public void registerStreamingCommand(String commandAlias, StreamingCommand command) {
        pigContext.registerStreamCmd(commandAlias, command);
    }

    private URL locateJarFromResources(String jarName) throws IOException {
        Enumeration<URL> urls = ClassLoader.getSystemResources(jarName);
        URL resourceLocation = null;
        
        if (urls.hasMoreElements()) {
            resourceLocation = urls.nextElement();
        }
        
        if (pigContext.debug && urls.hasMoreElements()) {
            String logMessage = "Found multiple resources that match " 
                + jarName + ": " + resourceLocation;
            
            while (urls.hasMoreElements()) {
                logMessage += (logMessage + urls.nextElement() + "; ");
            }
            
            log.debug(logMessage);
        }
    
        return resourceLocation;
    }
    
    /**
     * Registers a jar file. Name of the jar file can be an absolute or 
     * relative path.
     * 
     * If multiple resources are found with the specified name, the
     * first one is registered as returned by getSystemResources.
     * A warning is issued to inform the user.
     * 
     * @param name of the jar file to register
     * @throws IOException
     */
    public void registerJar(String name) throws IOException {
        // first try to locate jar via system resources
        // if this fails, try by using "name" as File (this preserves 
        // compatibility with case when user passes absolute path or path 
        // relative to current working directory.)        
        if (name != null) {
            URL resource = locateJarFromResources(name);

            if (resource == null) {
                File f = new File(name);
                
                if (!f.canRead()) {
                    int errCode = 4002;
                    String msg = "Can't read jar file: " + name;
                    throw new FrontendException(msg, errCode, PigException.USER_ENVIRONMENT);
                }
                
                resource = f.toURI().toURL();
            }

            pigContext.addJar(resource);        
        }
    }
    
    /**
     * Register a query with the Pig runtime. The query is parsed and registered, but it is not
     * executed until it is needed.
     * 
     * @param query
     *            a Pig Latin expression to be evaluated.
     * @param startLine
     *            line number of the query within the whold script
     * @throws IOException
     */    
    public void registerQuery(String query, int startLine) throws IOException {
            
        LogicalPlan lp = parseQuery(query, startLine, aliases, opTable, aliasOp);
        // store away the query for use in cloning later
        cachedScript .add(query);
        
        if (lp.getLeaves().size() == 1)
        {
            LogicalOperator op = lp.getSingleLeafPlanOutputOp();
            // No need to do anything about DEFINE 
            if (op instanceof LODefine) {
                return;
            }
        
            // Check if we just processed a LOStore i.e. STORE
            if (op instanceof LOStore) {
                try{
                    execute(null);
                } catch (Exception e) {
                    int errCode = 1002;
                    String msg = "Unable to store alias " + op.getOperatorKey().getId();
                    throw new FrontendException(msg, errCode, PigException.INPUT, e);
                }
            }
        }
    }
    
    private LogicalPlan parseQuery(String query, int startLine, Map<LogicalOperator, LogicalPlan> aliasesMap, 
            Map<OperatorKey, LogicalOperator> opTableMap, Map<String, LogicalOperator> aliasOpMap) throws IOException {
        if(query != null) {
            query = query.trim();
            if(query.length() == 0) return null;
        }else {
            return null;
        }
        try {
            return new LogicalPlanBuilder(pigContext).parse(scope, query,
                    aliasesMap, opTableMap, aliasOpMap, startLine);
        } catch (ParseException e) {
            //throw (IOException) new IOException(e.getMessage()).initCause(e);
            PigException pe = LogUtils.getPigException(e);
            int errCode = 1000;
            String msg = "Error during parsing. " + (pe == null? e.getMessage() : pe.getMessage());
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null, e);
        }
    }

    public LogicalPlan clonePlan(String alias) throws IOException {
        // There are two choices on how we clone the logical plan
        // 1 - we really clone each operator and connect up the cloned operators
        // 2 - we cache away the script till the point we need to clone
        // and then simply re-parse the script. 
        // The latter approach is used here
        // FIXME: There is one open issue with this now:
        // Consider the following script:
        // A = load 'file:/somefile';
        // B = filter A by $0 > 10;
        // store B into 'bla';
        // rm 'file:/somefile';
        // A = load 'file:/someotherfile'
        // when we try to clone - we try to reparse
        // from the beginning and currently the parser
        // checks for file existence of files in the load
        // in the case where the file is a local one -i.e. with file: prefix
        // This will be a known issue now and we will need to revisit later
        
        // parse each line of the cached script and the
        // final logical plan is the clone that we want
        LogicalPlan lp = null;
        int lineNumber = 1;
        // create data structures needed for parsing
        Map<LogicalOperator, LogicalPlan> cloneAliases = new HashMap<LogicalOperator, LogicalPlan>();
        Map<OperatorKey, LogicalOperator> cloneOpTable = new HashMap<OperatorKey, LogicalOperator>();
        Map<String, LogicalOperator> cloneAliasOp = new HashMap<String, LogicalOperator>();
        for (Iterator<String> it = cachedScript.iterator(); it.hasNext(); lineNumber++) {
            lp = parseQuery(it.next(), lineNumber, cloneAliases, cloneOpTable, cloneAliasOp);
        }
        
        if(alias == null) {
            // a store prompted the execution - so return
            // the entire logical plan
            return lp;
        } else {
            // return the logical plan corresponding to the 
            // alias supplied
            LogicalOperator op = cloneAliasOp.get(alias);
            if(op == null) {
                int errCode = 1003;
                String msg = "Unable to find an operator for alias " + alias;
                throw new FrontendException(msg, errCode, PigException.INPUT);
            }
            return cloneAliases.get(op);
        }
    }
    
    public void registerQuery(String query) throws IOException {
        registerQuery(query, 1);
    }
    
    public void registerScript(String fileName) throws IOException {
        try {
            GruntParser grunt = new GruntParser(new FileReader(new File(fileName)));
            grunt.setInteractive(false);
            grunt.setParams(this);
            grunt.parseStopOnError();
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new IOException(e.getCause());
        } catch (org.apache.pig.tools.pigscript.parser.ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            throw new IOException(e.getCause());
        }
    }

    public void printAliases () throws FrontendException {
        System.out.println("aliases: " + aliasOp.keySet());
    }

    public Schema dumpSchema(String alias) throws IOException{
        try {
            LogicalPlan lp = getPlanFromAlias(alias, "describe");
            lp = compileLp(alias, false);
            Schema schema = lp.getLeaves().get(0).getSchema();
            if (schema != null) System.out.println(alias + ": " + schema.toString());    
            else System.out.println("Schema for " + alias + " unknown.");
            return schema;
        } catch (FrontendException fee) {
            int errCode = 1001;
            String msg = "Unable to describe schema for alias " + alias; 
            throw new FrontendException (msg, errCode, PigException.INPUT, false, null, fee);
        }
    }

    public void setJobName(String name){
        pigContext.getProperties().setProperty(PigContext.JOB_NAME, PigContext.JOB_NAME_PREFIX + ":" + name);
    }
    
    /**
     * Forces execution of query (and all queries from which it reads), in order to materialize
     * result
     */
    public Iterator<Tuple> openIterator(String id) throws IOException {
        try {
            LogicalOperator op = aliasOp.get(id);
            if(null == op) {
                int errCode = 1003;
                String msg = "Unable to find an operator for alias " + id;
                throw new FrontendException(msg, errCode, PigException.INPUT);
            }
//            ExecJob job = execute(getPlanFromAlias(id, op.getClass().getName()));
            ExecJob job = store(id, FileLocalizer.getTemporaryPath(null, pigContext).toString(), BinStorage.class.getName() + "()");
            // invocation of "execute" is synchronous!

            if (job.getStatus() == JOB_STATUS.COMPLETED) {
                    return job.getResults();
            } else {
                throw new IOException("Job terminated with anomalous status "
                    + job.getStatus().toString());
            }
        } catch (Exception e) {
            int errCode = 1066;
            String msg = "Unable to open iterator for alias " + id; 
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        }
    }
    
    /**
     * Store an alias into a file
     * @param id The alias to store
     * @param filename The file to which to store to
     * @throws IOException
     */

    public ExecJob store(String id, String filename) throws IOException {
        return store(id, filename, PigStorage.class.getName() + "()");   // SFPig is the default store function
    }
        
    /**
     *  forces execution of query (and all queries from which it reads), in order to store result in file
     */
    public ExecJob store(
            String id,
            String filename,
            String func) throws IOException{
        if (!aliasOp.containsKey(id))
            throw new IOException("Invalid alias: " + id);
        
        try {
            LogicalPlan readFrom = getPlanFromAlias(id, "store");
            return store(id, readFrom, filename, func);
        } catch (FrontendException fe) {
            int errCode = 1002;
            String msg = "Unable to store alias " + id;
            throw new FrontendException(msg, errCode, PigException.INPUT, fe);
        }
    }
        
    public ExecJob store(
            String id,
            LogicalPlan readFrom,
            String filename,
            String func) throws IOException {
        try {
            LogicalPlan lp = compileLp(id);
            
            // MRCompiler needs a store to be the leaf - hence
            // add a store to the plan to explain
            
            // figure out the leaf to which the store needs to be added
            List<LogicalOperator> leaves = lp.getLeaves();
            LogicalOperator leaf = null;
            if(leaves.size() == 1) {
                leaf = leaves.get(0);
            } else {
                for (Iterator<LogicalOperator> it = leaves.iterator(); it.hasNext();) {
                    LogicalOperator leafOp = it.next();
                    if(leafOp.getAlias().equals(id))
                        leaf = leafOp;
                }
            }
            
            LogicalPlan storePlan = QueryParser.generateStorePlan(scope, lp, filename, func, leaf);
            return executeCompiledLogicalPlan(storePlan);
        } catch (Exception e) {
            int errCode = 1002;
            String msg = "Unable to store alias " + id;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        }

    }

    /**
     * Provide information on how a pig query will be executed.  For now
     * this information is very developer focussed, and probably not very
     * useful to the average user.
     * @param alias Name of alias to explain.
     * @param stream PrintStream to write explanation to.
     * @throws IOException if the requested alias cannot be found.
     */
    public void explain(String alias,
                        PrintStream stream) throws IOException {
        try {
            LogicalPlan lp = compileLp(alias);
            
            // MRCompiler needs a store to be the leaf - hence
            // add a store to the plan to explain
            
            // figure out the leaf to which the store needs to be added
            List<LogicalOperator> leaves = lp.getLeaves();
            LogicalOperator leaf = null;
            if(leaves.size() == 1) {
                leaf = leaves.get(0);
            } else {
                for (Iterator<LogicalOperator> it = leaves.iterator(); it.hasNext();) {
                    LogicalOperator leafOp = it.next();
                    if(leafOp.getAlias().equals(alias))
                        leaf = leafOp;
                }
            }
            
            LogicalPlan storePlan = QueryParser.generateStorePlan(
                scope, lp, "fakefile", PigStorage.class.getName(), leaf);
            stream.println("Logical Plan:");
            LOPrinter lv = new LOPrinter(stream, storePlan);
            lv.visit();

            PhysicalPlan pp = compilePp(storePlan);
            stream.println("-----------------------------------------------");
            stream.println("Physical Plan:");

            stream.println("-----------------------------------------------");
            pigContext.getExecutionEngine().explain(pp, stream);
      
        } catch (Exception e) {
            int errCode = 1067;
            String msg = "Unable to explain alias " + alias;
            throw new FrontendException(msg, errCode, PigException.INPUT, e);
        }
    }

    /**
     * Returns the unused byte capacity of an HDFS filesystem. This value does
     * not take into account a replication factor, as that can vary from file
     * to file. Thus if you are using this to determine if you data set will fit
     * in the HDFS, you need to divide the result of this call by your specific replication
     * setting. 
     * @return unused byte capacity of the file system.
     * @throws IOException
     */
    public long capacity() throws IOException {
        if (pigContext.getExecType() == ExecType.LOCAL) {
            throw new IOException("capacity only supported for non-local execution");
        } 
        else {
            DataStorage dds = pigContext.getDfs();
            
            Map<String, Object> stats = dds.getStatistics();

            String rawCapacityStr = (String) stats.get(DataStorage.RAW_CAPACITY_KEY);
            String rawUsedStr = (String) stats.get(DataStorage.RAW_USED_KEY);
            
            if ((rawCapacityStr == null) || (rawUsedStr == null)) {
                throw new IOException("Failed to retrieve capacity stats");
            }
            
            long rawCapacityBytes = new Long(rawCapacityStr).longValue();
            long rawUsedBytes = new Long(rawUsedStr).longValue();
            
            return rawCapacityBytes - rawUsedBytes;
        }
    }

    /**
     * Returns the length of a file in bytes which exists in the HDFS (accounts for replication).
     * @param filename
     * @return length of the file in bytes
     * @throws IOException
     */
    public long fileSize(String filename) throws IOException {
        DataStorage dfs = pigContext.getDfs();
        ElementDescriptor elem = dfs.asElement(filename);
        Map<String, Object> stats = elem.getStatistics();
        long length = (Long) stats.get(ElementDescriptor.LENGTH_KEY);
        int replication = (Short) stats
                .get(ElementDescriptor.BLOCK_REPLICATION_KEY);

        return length * replication;
    }
    
    public boolean existsFile(String filename) throws IOException {
        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        return elem.exists();
    }
    
    public boolean deleteFile(String filename) throws IOException {
        ElementDescriptor elem = pigContext.getDfs().asElement(filename);
        elem.delete();
        return true;
    }
    
    public boolean renameFile(String source, String target) throws IOException {
        pigContext.rename(source, target);
        return true;
    }
    
    public boolean mkdirs(String dirs) throws IOException {
        ContainerDescriptor container = pigContext.getDfs().asContainer(dirs);
        container.create();
        return true;
    }
    
    public String[] listPaths(String dir) throws IOException {
        Collection<String> allPaths = new ArrayList<String>();
        ContainerDescriptor container = pigContext.getDfs().asContainer(dir);
        Iterator<ElementDescriptor> iter = container.iterator();
            
        while (iter.hasNext()) {
            ElementDescriptor elem = iter.next();
            allPaths.add(elem.toString());
        }
            
        return (String[])(allPaths.toArray());
    }
    
    public long totalHadoopTimeSpent() {
//      TODO FIX Need to uncomment this with the right logic
//        return MapReduceLauncher.totalHadoopTimeSpent;
        return 0L;
    }
  
    public Map<String, LogicalPlan> getAliases() {
        Map<String, LogicalPlan> aliasPlans = new HashMap<String, LogicalPlan>();
        for(LogicalOperator op: this.aliases.keySet()) {
            String alias = op.getAlias();
            if(null != alias) {
                aliasPlans.put(alias, this.aliases.get(op));
            }
        }
        return aliasPlans;
    }
    
    public void shutdown() {
        // clean-up activities
            // TODO: reclaim scope to free up resources. Currently
        // this is not implemented and throws an exception
            // hence, for now, we won't call it.
        //
        // pigContext.getExecutionEngine().reclaimScope(this.scope);
    }

    public Map<LogicalOperator, DataBag> getExamples(String alias) {
        //LogicalPlan plan = aliases.get(aliasOp.get(alias));
        LogicalPlan plan = null;
        try {
            plan = clonePlan(alias);
        } catch (IOException e) {
            //Since the original script is parsed anyway, there should not be an
            //error in this parsing. The only reason there can be an error is when
            //the files being loaded in load don't exist anymore.
            e.printStackTrace();
        }
        ExampleGenerator exgen = new ExampleGenerator(plan, pigContext);
        return exgen.getExamples();
    }
    
    private ExecJob execute(String alias) throws FrontendException, ExecException {
        ExecJob job = null;
//        lp.explain(System.out, System.err);
        LogicalPlan typeCheckedLp = compileLp(alias);
        
        return executeCompiledLogicalPlan(typeCheckedLp);
//        typeCheckedLp.explain(System.out, System.err);
        
    }
    
    private ExecJob executeCompiledLogicalPlan(LogicalPlan compiledLp) throws ExecException {
        PhysicalPlan pp = compilePp(compiledLp);
        // execute using appropriate engine
        FileLocalizer.clearDeleteOnFail();
        ExecJob execJob = pigContext.getExecutionEngine().execute(pp, "execute");
        if (execJob.getStatus()==ExecJob.JOB_STATUS.FAILED)
            FileLocalizer.triggerDeleteOnFail();
        return execJob;
    }

    private LogicalPlan compileLp(
            String alias) throws FrontendException {
        return compileLp(alias, true);
    }

    private LogicalPlan compileLp(
            String alias,
            boolean optimize) throws FrontendException {
        
        // create a clone of the logical plan and give it
        // to the operations below
        LogicalPlan lpClone;
        try {
             lpClone = clonePlan(alias);
        } catch (IOException e) {
            int errCode = 2001;
            String msg = "Unable to clone plan before compiling";
            throw new FrontendException(msg, errCode, PigException.BUG, e);
        }

        
        // Set the logical plan values correctly in all the operators
        PlanSetter ps = new PlanSetter(lpClone);
        ps.visit();
        
        //(new SplitIntroducer(lp)).introduceImplSplits();
        
        // run through validator
        CompilationMessageCollector collector = new CompilationMessageCollector() ;
        FrontendException caught = null;
        try {
            LogicalPlanValidationExecutor validator = 
                new LogicalPlanValidationExecutor(lpClone, pigContext);
            validator.validate(lpClone, collector);
        } catch (FrontendException fe) {
            // Need to go through and see what the collector has in it.  But
            // remember what we've caught so we can wrap it into what we
            // throw.
            caught = fe;            
        }
        
        if(aggregateWarning) {
        	CompilationMessageCollector.logMessages(collector, MessageType.Warning, aggregateWarning, log);
        } else {
        	for(Enum type: MessageType.values()) {
        		CompilationMessageCollector.logAllMessages(collector, log);
        	}
        }
        
        if (caught != null) {
            throw caught;
        }

        // optimize
        if (optimize) {
            //LogicalOptimizer optimizer = new LogicalOptimizer(lpClone);
            LogicalOptimizer optimizer = new LogicalOptimizer(lpClone, pigContext.getExecType());
            optimizer.optimize();
        }

        return lpClone;
    }

    private PhysicalPlan compilePp(LogicalPlan lp) throws ExecException {
        // translate lp to physical plan
        PhysicalPlan pp = pigContext.getExecutionEngine().compile(lp, null);

        // TODO optimize

        return pp;
    }

    private LogicalPlan getPlanFromAlias(
            String alias,
            String operation) throws FrontendException {
        LogicalOperator lo = aliasOp.get(alias);
        if (lo == null) {
            int errCode = 1004;
            String msg = "No alias " + alias + " to " + operation;
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
        }
        LogicalPlan lp = aliases.get(lo);
        if (lp == null) {
            int errCode = 1005;
            String msg = "No plan for " + alias + " to " + operation;
            throw new FrontendException(msg, errCode, PigException.INPUT, false, null);
        }        
        return lp;
    }


}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.tools.grunt;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.FileReader;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.FileNotFoundException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Iterator;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.PrintStream;

import jline.ConsoleReader;
import jline.ConsoleReaderInputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.JobID;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigServer;
import org.apache.pig.backend.datastorage.ContainerDescriptor;
import org.apache.pig.backend.datastorage.DataStorage;
import org.apache.pig.backend.datastorage.DataStorageException;
import org.apache.pig.backend.datastorage.ElementDescriptor;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.tools.pigscript.parser.ParseException;
import org.apache.pig.tools.pigscript.parser.PigScriptParser;
import org.apache.pig.tools.pigscript.parser.PigScriptParserTokenManager;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.impl.util.LogUtils;

public class GruntParser extends PigScriptParser {

    private final Log log = LogFactory.getLog(getClass());

    public GruntParser(Reader stream) {
        super(stream);
        init();
    }

    public GruntParser(InputStream stream, String encoding) {
        super(stream, encoding);
        init();
    }

    public GruntParser(InputStream stream) {
        super(stream);
        init();
    }

    public GruntParser(PigScriptParserTokenManager tm) {
        super(tm);
        init();
    }

    private void init() {
        // nothing, for now.
    }
    
    public void parseStopOnError() throws IOException, ParseException
    {
        prompt();
        mDone = false;
        while(!mDone) {
            parse();
        }
    }

    public void parseContOnError()
    {
        prompt();
        mDone = false;
        while(!mDone) {
            try
            {
                parse();
            }
            catch(Exception e)
            {
                Exception pe = LogUtils.getPermissionException(e);
                if (pe != null)
                    log.error("You don't have permission to perform the operation. Error from the server: " + pe.getMessage());
                else {
                    ByteArrayOutputStream bs = new ByteArrayOutputStream();
                    e.printStackTrace(new PrintStream(bs));
                    log.error(bs.toString());
                    //log.error(e.getMessage());
                    //log.error(e);
               }

            } catch (Error e) {
                log.error(e);
            }
        }
    }

    public void setParams(PigServer pigServer)
    {
        mPigServer = pigServer;
        
        mDfs = mPigServer.getPigContext().getDfs();
        mLfs = mPigServer.getPigContext().getLfs();
        mConf = mPigServer.getPigContext().getProperties();
        
        // TODO: this violates the abstraction layer decoupling between
        // front end and back end and needs to be changed.
        // Right now I am not clear on how the Job Id comes from to tell
        // the back end to kill a given job (mJobClient is used only in 
        // processKill)
        //
        ExecutionEngine execEngine = mPigServer.getPigContext().getExecutionEngine();
        if (execEngine instanceof HExecutionEngine) {
            mJobClient = ((HExecutionEngine)execEngine).getJobClient();
        }
        else {
            mJobClient = null;
        }
    }

    public void prompt()
    {
        if (mInteractive)
        {
            /*System.err.print("grunt> ");
            System.err.flush();*/
            mConsoleReader.setDefaultPrompt("grunt> ");
        }
    }
    
    protected void quit()
    {
        mDone = true;
    }
    
    protected void processDescribe(String alias) throws IOException {
        if(alias==null) {
            alias = mPigServer.getPigContext().getLastAlias();
        }
        mPigServer.dumpSchema(alias);
    }

    protected void printAliases() throws IOException {
        mPigServer.printAliases();
    }


    protected void processExplain(String alias) throws IOException {
        mPigServer.explain(alias, System.out);
    }
    
    protected void processRegister(String jar) throws IOException {
        mPigServer.registerJar(jar);
    }

    private String runPreprocessor(String script, ArrayList<String> params, 
                                   ArrayList<String> files) 
        throws IOException, ParseException {

        ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
        StringWriter writer = new StringWriter();

        try{
            psp.genSubstitutedFile(new BufferedReader(new FileReader(script)), 
                                   writer,  
                                   params.size() > 0 ? params.toArray(new String[0]) : null, 
                                   files.size() > 0 ? files.toArray(new String[0]) : null);
        } catch (org.apache.pig.tools.parameters.ParseException pex) {
            throw new ParseException(pex.getMessage());
        }

        return writer.toString();
    }

    protected void processScript(String script, boolean batch, 
                                 ArrayList<String> params, ArrayList<String> files) 
        throws IOException, ParseException {
        
        Reader inputReader;
        ConsoleReader reader;
        boolean interactive;
         
        try {
            String cmds = runPreprocessor(script, params, files);

            if (mInteractive && !batch) { // Write prompt and echo commands
                reader = new ConsoleReader(new ByteArrayInputStream(cmds.getBytes()),
                                           new OutputStreamWriter(System.out));
                reader.setHistory(mConsoleReader.getHistory());
                InputStream in = new ConsoleReaderInputStream(reader);
                inputReader = new BufferedReader(new InputStreamReader(in));
                interactive = true;
            } else { // Quietly parse the statements
                inputReader = new StringReader(cmds);
                reader = null;
                interactive = false;
            }
        } catch (FileNotFoundException fnfe) {
            throw new ParseException("File not found: " + script);
        } catch (SecurityException se) {
            throw new ParseException("Cannot access file: " + script);
        }

        // In batch mode: Use a new server to avoid side-effects (handles, etc)
        PigServer pigServer = batch ? 
            new PigServer(mPigServer.getPigContext(), false) : mPigServer;
            
        GruntParser parser = new GruntParser(inputReader);
        parser.setParams(pigServer);
        parser.setConsoleReader(reader);
        parser.setInteractive(interactive);
        
        parser.parseStopOnError();
        if (interactive) {
            System.out.println("");
        }
    }

    protected void processSet(String key, String value) throws IOException, ParseException {
        if (key.equals("debug"))
        {
            if (value.equals("on") || value.equals("'on'"))
                mPigServer.debugOn();
            else if (value.equals("off") || value.equals("'off'"))
                mPigServer.debugOff();
            else
                throw new ParseException("Invalid value " + value + " provided for " + key);
        }
        else if (key.equals("job.name"))
        {
            //mPigServer.setJobName(unquote(value));
            mPigServer.setJobName(value);
        }
        else if (key.equals("stream.skippath")) {
            // Validate
            File file = new File(value);
            if (!file.exists() || file.isDirectory()) {
                throw new IOException("Invalid value for stream.skippath:" + 
                                      value); 
            }
            mPigServer.addPathToSkip(value);
        }
        else
        {
            // other key-value pairs can go there
            // for now just throw exception since we don't support
            // anything else
            throw new ParseException("Unrecognized set key: " + key);
        }
    }
    
    protected void processCat(String path) throws IOException
    {
        try {
            byte buffer[] = new byte[65536];
            ElementDescriptor dfsPath = mDfs.asElement(path);
            int rc;
            
            if (!dfsPath.exists())
                throw new IOException("Directory " + path + " does not exist.");
    
            if (mDfs.isContainer(path)) {
                ContainerDescriptor dfsDir = (ContainerDescriptor) dfsPath;
                Iterator<ElementDescriptor> paths = dfsDir.iterator();
                
                while (paths.hasNext()) {
                    ElementDescriptor curElem = paths.next();
                    
                    if (mDfs.isContainer(curElem.toString())) {
                        continue;
                    }
                    
                    InputStream is = curElem.open();
                    while ((rc = is.read(buffer)) > 0) {
                        System.out.write(buffer, 0, rc);
                    }
                    is.close();                
                }
            }
            else {
                InputStream is = dfsPath.open();
                while ((rc = is.read(buffer)) > 0) {
                    System.out.write(buffer, 0, rc);
                }
                is.close();            
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to Cat: " + path, e);
        }
    }

    protected void processCD(String path) throws IOException
    {    
        ContainerDescriptor container;

        try {
            if (path == null) {
                container = mDfs.asContainer("/user/" + System.getProperty("user.name"));
                mDfs.setActiveContainer(container);
            }
            else
            {
                container = mDfs.asContainer(path);
    
                if (!container.exists()) {
                    throw new IOException("Directory " + path + " does not exist.");
                }
                
                if (!mDfs.isContainer(path)) {
                    throw new IOException(path + " is not a directory.");
                }
                
                mDfs.setActiveContainer(container);
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to change working directory to " + 
                                  ((path == null) ? ("/user/" + System.getProperty("user.name")) 
                                                     : (path)), e);
        }
    }

    protected void processDump(String alias) throws IOException
    {
        Iterator result = mPigServer.openIterator(alias);
        while (result.hasNext())
        {
            Tuple t = (Tuple) result.next();
            System.out.println(t);
        }
    }
    
    protected void processIllustrate(String alias) throws IOException
    {
	mPigServer.getExamples(alias);
    }

    protected void processKill(String jobid) throws IOException
    {
        if (mJobClient != null) {
            JobID id = JobID.forName(jobid);
            RunningJob job = mJobClient.getJob(id);
            if (job == null)
                System.out.println("Job with id " + jobid + " is not active");
            else
            {    
                job.killJob();
                log.error("kill submited.");
            }
        }
    }
        
    protected void processLS(String path) throws IOException
    {
        try {
            ElementDescriptor pathDescriptor;
            
            if (path == null) {
                pathDescriptor = mDfs.getActiveContainer();
            }
            else {
                pathDescriptor = mDfs.asElement(path);
            }

            if (!pathDescriptor.exists()) {
                throw new IOException("File or directory " + path + " does not exist.");                
            }
            
            if (mDfs.isContainer(pathDescriptor.toString())) {
                ContainerDescriptor container = (ContainerDescriptor) pathDescriptor;
                Iterator<ElementDescriptor> elems = container.iterator();
                
                while (elems.hasNext()) {
                    ElementDescriptor curElem = elems.next();
                    
                    if (mDfs.isContainer(curElem.toString())) {
                           System.out.println(curElem.toString() + "\t<dir>");
                    } else {
                        printLengthAndReplication(curElem);
                    }
                }
            } else {
                printLengthAndReplication(pathDescriptor);
            }
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to LS on " + path, e);
        }
    }

    private void printLengthAndReplication(ElementDescriptor elem)
            throws IOException {
        Map<String, Object> stats = elem.getStatistics();

        long replication = (Short) stats
                .get(ElementDescriptor.BLOCK_REPLICATION_KEY);
        long len = (Long) stats.get(ElementDescriptor.LENGTH_KEY);

        System.out.println(elem.toString() + "<r " + replication + ">\t" + len);
    }
    
    protected void processPWD() throws IOException 
    {
        System.out.println(mDfs.getActiveContainer().toString());
    }

    protected void printHelp() 
    {
        System.out.println("Commands:");
        System.out.println("<pig latin statement>;");
        System.out.println("store <alias> into <filename> [using <functionSpec>]");
        System.out.println("dump <alias>");
        System.out.println("describe <alias>");
        System.out.println("kill <job_id>");
        System.out.println("ls <path>\r\ndu <path>\r\nmv <src> <dst>\r\ncp <src> <dst>\r\nrm <src>");
        System.out.println("copyFromLocal <localsrc> <dst>\r\ncd <dir>\r\npwd");
        System.out.println("cat <src>\r\ncopyToLocal <src> <localdst>\r\nmkdir <path>");
        System.out.println("cd <path>");
        System.out.println("define <functionAlias> <functionSpec>");
        System.out.println("register <udfJar>");
        System.out.println("set key value");
        System.out.println("quit");
    }

    protected void processMove(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            if (!srcPath.exists()) {
                throw new IOException("File or directory " + src + " does not exist.");                
            }
            
            srcPath.rename(dstPath);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to move " + src + " to " + dst, e);
        }
    }
    
    protected void processCopy(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, mConf, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy " + src + " to " + dst, e);
        }
    }
    
    protected void processCopyToLocal(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mDfs.asElement(src);
            ElementDescriptor dstPath = mLfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy " + src + "to (locally) " + dst, e);
        }
    }

    protected void processCopyFromLocal(String src, String dst) throws IOException
    {
        try {
            ElementDescriptor srcPath = mLfs.asElement(src);
            ElementDescriptor dstPath = mDfs.asElement(dst);
            
            srcPath.copy(dstPath, false);
        }
        catch (DataStorageException e) {
            throw WrappedIOException.wrap("Failed to copy (loally) " + src + "to " + dst, e);
        }
    }
    
    protected void processMkdir(String dir) throws IOException
    {
        ContainerDescriptor dirDescriptor = mDfs.asContainer(dir);
        dirDescriptor.create();
    }
    
    protected void processPig(String cmd) throws IOException
    {
        int start = 1;
        if (!mInteractive)
            start = getLineNumber();
        if (cmd.charAt(cmd.length() - 1) != ';')
            mPigServer.registerQuery(cmd + ";", start); 
        else 
            mPigServer.registerQuery(cmd, start);
    }

    protected void processRemove(String path, String options ) throws IOException
    {
        ElementDescriptor dfsPath = mDfs.asElement(path);
        
        if (!dfsPath.exists()) {
            if (options == null || !options.equalsIgnoreCase("force")) {
                throw new IOException("File or directory " + path + " does not exist."); 
            }
        }
        else {
            
            dfsPath.delete();
        }
    }

    private PigServer mPigServer;
    private DataStorage mDfs;
    private DataStorage mLfs;
    private Properties mConf;
    private JobClient mJobClient;
    private boolean mDone;

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.builtin;

import java.io.IOException;
import java.util.Map;
import java.io.ByteArrayInputStream;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

import org.apache.pig.PigException;
import org.apache.pig.PigWarning;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PigLogger;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.parser.ParseException;
import org.apache.pig.data.parser.TextDataParser;
import org.apache.pig.impl.util.LogUtils;

/**
 * This abstract class provides standard conversions between utf8 encoded data
 * and pig data types.  It is intended to be extended by load and store
 * functions (such as PigStorage). 
 */
abstract public class Utf8StorageConverter {

    protected BagFactory mBagFactory = BagFactory.getInstance();
    protected TupleFactory mTupleFactory = TupleFactory.getInstance();
    protected final Log mLog = LogFactory.getLog(getClass());

    private Integer mMaxInt = new Integer(Integer.MAX_VALUE);
    private Long mMaxLong = new Long(Long.MAX_VALUE);
    private TextDataParser dataParser = null;
    
    private PigLogger pigLogger = PhysicalOperator.getPigLogger();
        
    public Utf8StorageConverter() {
    }

    private Object parseFromBytes(byte[] b) throws ParseException {
        ByteArrayInputStream in = new ByteArrayInputStream(b);
        if(dataParser == null) {
            dataParser = new TextDataParser(in);
        } else {
            dataParser.ReInit(in);
        }
        return dataParser.Parse();
    }

    public DataBag bytesToBag(byte[] b) throws IOException {
        if(b == null)
            return null;
        DataBag db;
        try {
            db = (DataBag)parseFromBytes(b);
        } catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type bag, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type bag, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return (DataBag)db;
    }

    public String bytesToCharArray(byte[] b) throws IOException {
        if(b == null)
            return null;
        return new String(b, "UTF-8");
    }

    public Double bytesToDouble(byte[] b) {
        if(b == null)
            return null;
        try {
            return Double.valueOf(new String(b));
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to double, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }

    public Float bytesToFloat(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s;
        if(b.length > 0 && 
           (b[b.length - 1] == 'F' || b[b.length - 1] == 'f') ){
            s = new String(b, 0, b.length - 1);
        } 
        else {
            s = new String(b);
        }
        
        try {
            return Float.valueOf(s);
        } catch (NumberFormatException nfe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to float, caught NumberFormatException <" +
                    nfe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;
        }
    }

    public Integer bytesToInteger(byte[] b) throws IOException {
        if(b == null)
            return null;
        String s = new String(b);
        try {
            return Integer.valueOf(s);
        } catch (NumberFormatException nfe) {
            // It's possible that this field can be interpreted as a double.
            // Unfortunately Java doesn't handle this in Integer.valueOf.  So
            // we need to try to convert it to a double and if that works then
            // go to an int.
            try {
                Double d = Double.valueOf(s);
                // Need to check for an overflow error
                if (d.doubleValue() > mMaxInt.doubleValue() + 1.0) {
                    LogUtils.warn(this, "Value " + d + " too large for integer", 
                                PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return new Integer(d.intValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                        "converted to int, caught NumberFormatException <" +
                        nfe.getMessage() + "> field discarded", 
                        PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }

    public Long bytesToLong(byte[] b) throws IOException {
        if(b == null)
            return null;

        String s;
        if(b.length > 0  &&  
           (b[b.length - 1] == 'L' || b[b.length - 1] == 'l') ){
            s = new String(b, 0, b.length - 1);
        } 
        else {
            s = new String(b);
        }

        try {
            return Long.valueOf(s);
        } catch (NumberFormatException nfe) {
            // It's possible that this field can be interpreted as a double.
            // Unfortunately Java doesn't handle this in Long.valueOf.  So
            // we need to try to convert it to a double and if that works then
            // go to an long.
            try {
                Double d = Double.valueOf(s);
                // Need to check for an overflow error
                if (d.doubleValue() > mMaxLong.doubleValue() + 1.0) {
                	LogUtils.warn(this, "Value " + d + " too large for integer", 
                	            PigWarning.TOO_LARGE_FOR_INT, mLog);
                    return null;
                }
                return new Long(d.longValue());
            } catch (NumberFormatException nfe2) {
                LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                            "converted to long, caught NumberFormatException <" +
                            nfe.getMessage() + "> field discarded", 
                            PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
                return null;
            }
        }
    }

    public Map<Object, Object> bytesToMap(byte[] b) throws IOException {
        if(b == null)
            return null;
        Map<Object, Object> map;
        try {
            map = (Map<Object, Object>)parseFromBytes(b);
        }
        catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type map, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type map, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return map;
    }

    public Tuple bytesToTuple(byte[] b) throws IOException {
        if(b == null)
            return null;
        Tuple t;
        try {
            t = (Tuple)parseFromBytes(b);
        } 
        catch (ParseException pe) {
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type tuple, caught ParseException <" +
                    pe.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }catch (Exception e){
            // can happen if parseFromBytes identifies it as being of different type
            LogUtils.warn(this, "Unable to interpret value " + b + " in field being " +
                    "converted to type tuple, caught Exception <" +
                    e.getMessage() + "> field discarded", 
                    PigWarning.FIELD_DISCARDED_TYPE_CONVERSION_FAILED, mLog);
            return null;       
        }
        return t;
    }


    public byte[] toBytes(DataBag bag) throws IOException {
        return bag.toString().getBytes();
    }

    public byte[] toBytes(String s) throws IOException {
        return s.getBytes();
    }

    public byte[] toBytes(Double d) throws IOException {
        return d.toString().getBytes();
    }

    public byte[] toBytes(Float f) throws IOException {
        return f.toString().getBytes();
    }

    public byte[] toBytes(Integer i) throws IOException {
        return i.toString().getBytes();
    }

    public byte[] toBytes(Long l) throws IOException {
        return l.toString().getBytes();
    }

    public byte[] toBytes(Map<Object, Object> m) throws IOException {
        return DataType.mapToString(m).getBytes();
    }

    public byte[] toBytes(Tuple t) throws IOException {
        return t.toString().getBytes();
    }
    


}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.mapReduceLayer;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskReport;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.pig.FuncSpec;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.executionengine.ExecutionEngine;
import org.apache.pig.backend.hadoop.datastorage.HConfiguration;
import org.apache.pig.backend.hadoop.executionengine.HExecutionEngine;
import org.apache.pig.impl.PigContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.LogUtils;

public abstract class Launcher {
    private static final Log log = LogFactory.getLog(Launcher.class);
    
    long totalHadoopTimeSpent;    
    String newLine = "\n";
    boolean pigException = false;
    boolean outOfMemory = false;
    final String OOM_ERR = "OutOfMemoryError";
    
    protected Launcher(){
        totalHadoopTimeSpent = 0;
        //handle the windows portion of \r
        if (System.getProperty("os.name").toUpperCase().startsWith("WINDOWS")) {
            newLine = "\r\n";
        }
    }
    /**
     * Method to launch pig for hadoop either for a cluster's
     * job tracker or for a local job runner. THe only difference
     * between the two is the job client. Depending on the pig context
     * the job client will be initialize to one of the two.
     * Launchers for other frameworks can overide these methods.
     * Given an input PhysicalPlan, it compiles it
     * to get a MapReduce Plan. The MapReduce plan which
     * has multiple MapReduce operators each one of which
     * has to be run as a map reduce job with dependency
     * information stored in the plan. It compiles the
     * MROperPlan into a JobControl object. Each Map Reduce
     * operator is converted into a Job and added to the JobControl
     * object. Each Job also has a set of dependent Jobs that
     * are created using the MROperPlan.
     * The JobControl object is obtained from the JobControlCompiler
     * Then a new thread is spawned that submits these jobs
     * while respecting the dependency information.
     * The parent thread monitors the submitted jobs' progress and
     * after it is complete, stops the JobControl thread.
     * @param php
     * @param grpName
     * @param pc
     * @throws PlanException
     * @throws VisitorException
     * @throws IOException
     * @throws ExecException
     * @throws JobCreationException
     */
    public abstract boolean launchPig(PhysicalPlan php, String grpName, PigContext pc)
            throws PlanException, VisitorException, IOException, ExecException,
            JobCreationException, Exception;

    /**
     * Explain how a pig job will be executed on the underlying
     * infrastructure.
     * @param pp PhysicalPlan to explain
     * @param pc PigContext to use for configuration
     * @param ps PrintStream to write output on.
     * @throws VisitorException
     * @throws IOException
     */
    public abstract void explain(
            PhysicalPlan pp,
            PigContext pc,
            PrintStream ps) throws PlanException,
                                   VisitorException,
                                   IOException;
    
    protected boolean isComplete(double prog){
        return (int)(Math.ceil(prog)) == (int)1;
    }
    
    protected void getStats(Job job, JobClient jobClient, boolean errNotDbg, PigContext pigContext) throws Exception {
        JobID MRJobID = job.getAssignedJobID();
        String jobMessage = job.getMessage();
        if(MRJobID == null) {
        	try {
                throw getExceptionFromString(jobMessage);
            } catch (Exception e) {
                //just get the first line in the message and log the rest
                String firstLine = getFirstLineFromMessage(jobMessage);

                LogUtils.writeLog(new Exception(jobMessage), pigContext.getProperties().getProperty("pig.logfile"), 
                        log, false, null, false, false);
                int errCode = 2997;
                String msg = "Unable to recreate exception from backend error: " + firstLine;
                throw new ExecException(msg, errCode, PigException.BUG, e);
            } 
        }
        try {
            TaskReport[] mapRep = jobClient.getMapTaskReports(MRJobID);
            getErrorMessages(mapRep, "map", errNotDbg, pigContext);
            totalHadoopTimeSpent += computeTimeSpent(mapRep);
            TaskReport[] redRep = jobClient.getReduceTaskReports(MRJobID);
            getErrorMessages(redRep, "reduce", errNotDbg, pigContext);
            totalHadoopTimeSpent += computeTimeSpent(mapRep);
        } catch (IOException e) {
            if(job.getState() == Job.SUCCESS) {
                // if the job succeeded, let the user know that
                // we were unable to get statistics
                log.warn("Unable to get job related diagnostics");
            } else {
                throw e;
            }
        }
    }
    
    protected long computeTimeSpent(TaskReport[] mapReports) {
        long timeSpent = 0;
        for (TaskReport r : mapReports) {
            timeSpent += (r.getFinishTime() - r.getStartTime());
        }
        return timeSpent;
    }
    
    
    protected void getErrorMessages(TaskReport reports[], String type, boolean errNotDbg, PigContext pigContext) throws Exception
    {        
    	for (int i = 0; i < reports.length; i++) {
            String msgs[] = reports[i].getDiagnostics();
            ArrayList<Exception> exceptions = new ArrayList<Exception>();
            boolean jobFailed = false;
            float successfulProgress = 1.0f;
            if (msgs.length > 0) {
            	//if the progress reported is not 1.0f then the map or reduce job failed
            	//this comparison is in place till Hadoop 0.20 provides methods to query
            	//job status            	
            	if(reports[i].getProgress() != successfulProgress) {
            		jobFailed = true;
            	}
                Set<String> errorMessageSet = new HashSet<String>();
                for (int j = 0; j < msgs.length; j++) {                	
	            	if(!errorMessageSet.contains(msgs[j])) {
	            	    errorMessageSet.add(msgs[j]);
		            	if (errNotDbg) {
		            		//errNotDbg is used only for failed jobs
		            	    //keep track of all the unique exceptions
                            try {
                                Exception e = getExceptionFromString(msgs[j]);
                                exceptions.add(e);
                            } catch (Exception e1) {
                                String firstLine = getFirstLineFromMessage(msgs[j]);                                
                                LogUtils.writeLog(new Exception(msgs[j]), pigContext.getProperties().getProperty("pig.logfile"), 
                                        log, false, null, false, false);
                                int errCode = 2997;
                                String msg = "Unable to recreate exception from backed error: " + firstLine;
                                throw new ExecException(msg, errCode, PigException.BUG, e1);
                            }
		                } else {
		                    log.debug("Error message from task (" + type + ") " +
		                        reports[i].getTaskID() + msgs[j]);
		                }
	            	}
	            }
            }
            
            //if its a failed job then check if there is more than one exception
            //more than one exception implies possibly different kinds of failures
            //log all the different failures and throw the exception corresponding
            //to the first failure
            if(jobFailed) {
                if(exceptions.size() > 1) {
                    for(int j = 0; j < exceptions.size(); ++j) {
                        String headerMessage = "Error message from task (" + type + ") " + reports[i].getTaskID();
                        LogUtils.writeLog(exceptions.get(j), pigContext.getProperties().getProperty("pig.logfile"), log, false, headerMessage, false, false);
                    }
                    throw exceptions.get(0);
                } else if(exceptions.size() == 1) {
                	throw exceptions.get(0);
                } else {
                	int errCode = 2115;
                	String msg = "Internal error. Expected to throw exception from the backend. Did not find any exception to throw.";
                	throw new ExecException(msg, errCode, PigException.BUG);
                }
            }
        }
    }
    
    /**
     * Compute the progress of the current job submitted 
     * through the JobControl object jc to the JobClient jobClient
     * @param jc - The JobControl object that has been submitted
     * @param jobClient - The JobClient to which it has been submitted
     * @return The progress as a precentage in double format
     * @throws IOException
     */
    protected double calculateProgress(JobControl jc, JobClient jobClient) throws IOException{
        double prog = 0.0;
        prog += jc.getSuccessfulJobs().size();
        
        List runnJobs = jc.getRunningJobs();
        for (Object object : runnJobs) {
            Job j = (Job)object;
            prog += progressOfRunningJob(j, jobClient);
        }
        return prog;
    }
    
    /**
     * Returns the progress of a Job j which is part of a submitted
     * JobControl object. The progress is for this Job. So it has to
     * be scaled down by the num of jobs that are present in the 
     * JobControl.
     * @param j - The Job for which progress is required
     * @param jobClient - the JobClient to which it has been submitted
     * @return Returns the percentage progress of this Job
     * @throws IOException
     */
    protected double progressOfRunningJob(Job j, JobClient jobClient) throws IOException{
        JobID mrJobID = j.getAssignedJobID();
        RunningJob rj = jobClient.getJob(mrJobID);
        if(rj==null && j.getState()==Job.SUCCESS)
            return 1;
        else if(rj==null)
            return 0;
        else{
            double mapProg = rj.mapProgress();
            double redProg = rj.reduceProgress();
            return (mapProg + redProg)/2;
        }
    }
    public long getTotalHadoopTimeSpent() {
        return totalHadoopTimeSpent;
    }


    /**
     * 
     * @param stackTraceLine The string representation of {@link Throwable#printStackTrace() printStackTrace}
     * Handles internal PigException and its subclasses that override the {@link Throwable#toString() toString} method
     * @return An exception object whose string representation of printStackTrace is the input stackTrace 
     * @throws Exception
     */
    Exception getExceptionFromString(String stackTrace) throws Exception{
        String[] lines = stackTrace.split(newLine);
        Throwable t = getExceptionFromStrings(lines, 0);
        
        if(!pigException) {
            int errCode = 6015;
            String msg = "During execution, encountered a Hadoop error.";
            ExecException ee = new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, t);
            ee.setStackTrace(t.getStackTrace());
            return ee;
        } else {
            pigException = false;
            if(outOfMemory) {
                outOfMemory = false;
                int errCode = 6016;
                String msg = "Out of memory.";
                ExecException ee = new ExecException(msg, errCode, PigException.REMOTE_ENVIRONMENT, t);
                ee.setStackTrace(t.getStackTrace());
                return ee;
            }
            return (Exception)t;
        }
    }

    /**
     * 
     * @param stackTraceLine An array of strings that represent {@link Throwable#printStackTrace() printStackTrace}
     * output, split by newline
     * @return An exception object whose string representation of printStackTrace is the input stackTrace 
     * @throws Exception
     */
    private Throwable getExceptionFromStrings(String[] stackTraceLines, int startingLineNum) throws Exception{
        /*
         * parse the array of string and throw the appropriate exception
         * first: from the line startingLineNum extract the exception name extract the message if any
         * fourth: create the appropriate exception and return it
         * An example of the stack trace:
		org.apache.pig.backend.executionengine.ExecException: ERROR 1075: Received a bytearray from the UDF. Cannot determine how to convert the bytearray to int.
        at org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POCast.getNext(POCast.java:152)
        at org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.LessThanExpr.getNext(LessThanExpr.java:85)
        at org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter.getNext(POFilter.java:148)
        at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase.runPipeline(PigMapBase.java:184)
        at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapBase.map(PigMapBase.java:174)
        at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map(PigMapOnly.java:65)
        at org.apache.hadoop.mapred.MapRunner.run(MapRunner.java:47)
        at org.apache.hadoop.mapred.MapTask.run(MapTask.java:227)
        at org.apache.hadoop.mapred.TaskTracker$Child.main(TaskTracker.java:2207)
         */

        int prevStartingLineNum = startingLineNum;
        
        if(stackTraceLines.length > 0 && startingLineNum < (stackTraceLines.length - 1)) {
            
            //the regex for matching the exception class name; note the use of the $ for matching nested classes
            String exceptionNameDelimiter = "(\\w+(\\$\\w+)?\\.)+\\w+";
            Pattern exceptionNamePattern = Pattern.compile(exceptionNameDelimiter);
            
        	//from the first line extract the exception name and the exception message
            Matcher exceptionNameMatcher = exceptionNamePattern.matcher(stackTraceLines[startingLineNum]);
            String exceptionName = null;
            String exceptionMessage = null;
            if (exceptionNameMatcher.find()) {
            	exceptionName = exceptionNameMatcher.group();
            	/*
            	 * note that the substring is from end + 2
            	 * the regex matcher ends at one position beyond the match
            	 * in this case it will end at colon (:)
            	 * the exception message will have a preceding space (after the colon (:)) 
            	 */ 
            	if (exceptionName.contains(OOM_ERR)) {
            	    outOfMemory = true;
            	}
            	
            	if(stackTraceLines[startingLineNum].length() > exceptionNameMatcher.end()) {
	            	exceptionMessage = stackTraceLines[startingLineNum].substring(exceptionNameMatcher.end() + 2);
            	}
            	
            	++startingLineNum;
            }
        	
            //the exceptionName should not be null
            if(exceptionName != null) {            	

                ArrayList<StackTraceElement> stackTraceElements = new ArrayList<StackTraceElement>();
                
                //Create stack trace elements for the remaining lines
                String stackElementRegex = "\\s+at\\s+(\\w+(\\$\\w+)?\\.)+(\\<)?\\w+(\\>)?";
                Pattern stackElementPattern = Pattern.compile(stackElementRegex);
                String pigExceptionRegex = "org\\.apache\\.pig\\.";
                Pattern pigExceptionPattern = Pattern.compile(pigExceptionRegex);              
                String moreElementRegex = "\\s+\\.\\.\\.\\s+\\d+\\s+more";
                Pattern moreElementPattern = Pattern.compile(moreElementRegex);
                
                
                String pigPackageRegex = "org.apache.pig";
                
                int lineNum = startingLineNum;
                for(; lineNum < (stackTraceLines.length - 1); ++lineNum) {
                    Matcher stackElementMatcher = stackElementPattern.matcher(stackTraceLines[lineNum]);

                    if(stackElementMatcher.find()) {
                        StackTraceElement ste = getStackTraceElement(stackTraceLines[lineNum]);
                        stackTraceElements.add(ste);
                        String className = ste.getClassName();
                        Matcher pigExceptionMatcher = pigExceptionPattern.matcher(className);
                        if(pigExceptionMatcher.find()) {
                            pigException = true;
                        }                       
                    } else {
                        Matcher moreElementMatcher = moreElementPattern.matcher(stackTraceLines[lineNum]);
                        if(moreElementMatcher.find()) {
                            ++lineNum;
                        }
                        break;
                    }
                }
                
                startingLineNum = lineNum;               

            	//create the appropriate exception; setup the stack trace and message
            	Object object = PigContext.instantiateFuncFromSpec(exceptionName);
            	
            	if(object instanceof PigException) {
            		//extract the error code and message the regex for matching the custom format of ERROR <ERROR CODE>:
            		String errMessageRegex = "ERROR\\s+\\d+:";
            		Pattern errMessagePattern = Pattern.compile(errMessageRegex);
            		Matcher errMessageMatcher = errMessagePattern.matcher(exceptionMessage);
            		
            		if(errMessageMatcher.find()) {
            			String errMessageStub = errMessageMatcher.group();
            			/*
            			 * extract the actual exception message sans the ERROR <ERROR CODE>:
            			 * again note that the matcher ends at the space following the colon (:)
            			 * the exception message appears after the space and hence the end + 1
            			 */            			
            			exceptionMessage = exceptionMessage.substring(errMessageMatcher.end() + 1);
                		
            			//the regex to match the error code wich is a string of numerals
            			String errCodeRegex = "\\d+";
                		Pattern errCodePattern = Pattern.compile(errCodeRegex);
                		Matcher errCodeMatcher = errCodePattern.matcher(errMessageStub);
                		
                		String code = null;
                		if(errCodeMatcher.find()) {
                			code = errCodeMatcher.group();	
                		}
            			
                		//could receive a number format exception here but it will be propagated up the stack                		
                		int errCode = Integer.parseInt(code);
                		
                		//create the exception with the message and then set the error code and error source
                		FuncSpec funcSpec = new FuncSpec(exceptionName, exceptionMessage);		                		
                		object = PigContext.instantiateFuncFromSpec(funcSpec);
                		((PigException)object).setErrorCode(errCode);
                		((PigException)object).setErrorSource(PigException.determineErrorSource(errCode));
            		} else { //else for if(errMessageMatcher.find())
            			/*
            			 * did not find the error code which means that the PigException or its
            			 * subclass is not returning the error code
            			 * highly unlikely: should never be here
            			 */
            			FuncSpec funcSpec = new FuncSpec(exceptionName, exceptionMessage);		                		
                		object = PigContext.instantiateFuncFromSpec(funcSpec);
                		((PigException)object).setErrorCode(2997);//generic error code
                		((PigException)object).setErrorSource(PigException.BUG);		                			
            		}		                		
            	} else { //else for if(object instanceof PigException)
            		//its not PigException; create the exception with the message
            		object = PigContext.instantiateFuncFromSpec(exceptionName + "(" + exceptionMessage + ")");
            	}
            	
            	StackTraceElement[] steArr = new StackTraceElement[stackTraceElements.size()];
            	((Throwable)object).setStackTrace((StackTraceElement[])(stackTraceElements.toArray(steArr)));
            	
            	if(startingLineNum < (stackTraceLines.length - 1)) {
            	    Throwable e = getExceptionFromStrings(stackTraceLines, startingLineNum);
            	    ((Throwable)object).initCause(e);
            	}
            	
            	return (Throwable)object;
            } else { //else for if(exceptionName != null)
        		int errCode = 2055;
        		String msg = "Did not find exception name to create exception from string: " + stackTraceLines.toString();
        		throw new ExecException(msg, errCode, PigException.BUG);
            }
        } else { //else for if(lines.length > 0)
    		int errCode = 2056;
    		String msg = "Cannot create exception from empty string.";
    		throw new ExecException(msg, errCode, PigException.BUG);
        }
    }
    
    /**
     * 
     * @param line the string representation of a stack trace returned by {@link Throwable#printStackTrace() printStackTrace}
     * @return the StackTraceElement object representing the stack trace
     * @throws Exception
     */
    public StackTraceElement getStackTraceElement(String line) throws Exception{
    	/*    	
    	 * the format of the line is something like:
    	 *     	        at org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map(PigMapOnly.java:65)
    	 * note the white space before the 'at'. Its not of much importance but noted for posterity.    
    	 */
    	String[] items;
    	
    	/*
    	 * regex for matching the fully qualified method Name
    	 * note the use of the $ for matching nested classes
    	 * and the use of < and > for constructors
    	 */
    	String qualifiedMethodNameRegex = "(\\w+(\\$\\w+)?\\.)+(<)?\\w+(>)?";
        Pattern qualifiedMethodNamePattern = Pattern.compile(qualifiedMethodNameRegex);
        Matcher contentMatcher = qualifiedMethodNamePattern.matcher(line);
        
        //org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map(PigMapOnly.java:65)
    	String content = null;
        if(contentMatcher.find()) {
        	content = line.substring(contentMatcher.start());
        } else {
    		int errCode = 2057;
    		String msg = "Did not find fully qualified method name to reconstruct stack trace: " + line;
    		throw new ExecException(msg, errCode, PigException.BUG);        	
        }
        
        Matcher qualifiedMethodNameMatcher = qualifiedMethodNamePattern.matcher(content);
        
        //org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapOnly$Map.map
        String qualifiedMethodName = null;
        //(PigMapOnly.java:65)
        String fileDetails = null;
        
        if(qualifiedMethodNameMatcher.find()) {
        	qualifiedMethodName = qualifiedMethodNameMatcher.group();
        	fileDetails = content.substring(qualifiedMethodNameMatcher.end() + 1);
        } else {
    		int errCode = 2057;
    		String msg = "Did not find fully qualified method name to reconstruct stack trace: " + line;
    		throw new ExecException(msg, errCode, PigException.BUG);        	
        }
    	
        //From the fully qualified method name, extract the declaring class and method name
        items = qualifiedMethodName.split("\\.");
        
        //initialize the declaringClass (to org in most cases)
        String declaringClass = items[0]; 
        //the last member is always the method name
        String methodName = items[items.length - 1];
        
        //concatenate the names by adding the dot (.) between the members till the penultimate member
        for(int i = 1; i < items.length - 1; ++i) {
        	declaringClass += ".";
        	declaringClass += items[i];
        }
        
        //from the file details extract the file name and the line number
        //PigMapOnly.java:65
        fileDetails = fileDetails.substring(0, fileDetails.length() - 1);
        items = fileDetails.split(":");
        //PigMapOnly.java
        String fileName = null;
        int lineNumber = 0;
        if(items.length > 0) {
            fileName = items[0];
            lineNumber = Integer.parseInt(items[1]);
        }
        return new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
    }
    
    private String getFirstLineFromMessage(String message) {
        String[] messages = message.split(newLine);
        if(messages.length > 0) {
            return messages[0];
        } else {
            return new String(message);
        }        
    }

}

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.test;

import org.junit.Test;
import junit.framework.TestCase;
import junit.framework.AssertionFailedError;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.ExecType;
import org.apache.pig.PigException;
import org.apache.pig.PigServer;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.impl.util.LogUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;

public class TestGrunt extends TestCase {
    MiniCluster cluster = MiniCluster.buildCluster();
    private String basedir;

    private final Log log = LogFactory.getLog(getClass());

    public TestGrunt(String name) {
        super(name);
        basedir = "test/org/apache/pig/test/data";
    }
    
/*    @Test 
    public void testCopyFromLocal() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "copyFromLocal /bin/sh . ;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }*/

    @Test 
    public void testDefine() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "define myudf org.apache.pig.builtin.AVG();\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        try {
            grunt.exec();
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Encountered \"define\""));
        }
        assertTrue(null != context.getFuncSpecFromAlias("myudf"));
    }

    @Test 
    public void testBagSchema() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1' as (b: bag{t(i: int, c:chararray, f: float)});\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagSchemaFail() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'as (b: bag{t(i: int, c:chararray, f: float)});\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        try {
            grunt.exec();
        } catch (Exception e) {
            PigException pe = LogUtils.getPigException(e);
            String msg = (pe == null? e.getMessage(): pe.getMessage());
            assertTrue(msg.contains("Encountered \" \";"));
        }
    }

    @Test 
    public void testBagConstant() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchema() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchemaInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast) as fast;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast) as fast;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast);};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast);\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock2() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "cash = load 'foo' as (foo, fast); b = foreach cash generate fast * 2.0;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }


    @Test 
    public void testParsingGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate a.regenerate;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate a.regenerate;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testRunStatment() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate);" +
                        " run -param LIMIT=5 -param_file " + basedir +
                        "/test_broken.ppf " + basedir + "/testsub.pig; explain bar";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExecStatment() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        boolean caught = false;
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate);" +
                        " exec -param LIMIT=5 -param FUNCTION=COUNT " +
                        "-param FILE=foo " + basedir + "/testsub.pig; explain bar";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        
        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("alias bar"));
        }
        assertTrue(caught);
    }

    @Test
    public void testRunStatmentNested() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); run "
            +basedir+"/testsubnested_run.pig; explain bar";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExecStatmentNested() throws Throwable {
        PigServer server = new PigServer("MAPREDUCE");
        PigContext context = server.getPigContext();
        boolean caught = false;
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); exec "
            +basedir+"/testsubnested_exec.pig; explain bar";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
        
        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("alias bar"));
        }
        assertTrue(caught);
    }
}

