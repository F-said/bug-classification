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
import org.apache.pig.tools.pigscript.parser.ParseException;
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
        cluster.setProperty("opt.multiquery","true");
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1' as (b: bag{t(i: int, c:chararray, f: float)});\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagSchemaFail() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchema() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testBagConstantWithSchemaInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'input1'; b = foreach a {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, c:chararray, d: double)};};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast) as fast;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast) as fast;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b {generate SUM(a.fast);};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast); b = group a by foo; c = foreach b generate SUM(a.fast);\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingWordWithAsInForeachWithOutBlock2() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "cash = load 'foo' as (foo, fast); b = foreach cash generate fast * 2.0;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }


    @Test 
    public void testParsingGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate a.regenerate;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate a.regenerate;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b {generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;};\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test 
    public void testParsingAsGenerateInForeachWithOutBlock() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); b = group a by foo; c = foreach b generate {(1, '1', 0.4f),(2, '2', 0.45)} as b: bag{t(i: int, cease:chararray, degenerate: double)}, SUM(a.fast) as fast, a.regenerate as degenerated;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testRunStatment() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        boolean caught = false;
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate);" +
                        " exec -param LIMIT=5 -param FUNCTION=COUNT " +
                        "-param FILE=foo " + basedir + "/testsub.pig; explain bar;";
        
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
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
    
    @Test
    public void testExplainEmpty() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); run "
            +basedir+"/testsubnested_run.pig; explain";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExplainScript() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -script "
            +basedir+"/testsubnested_run.pig;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExplainBrief() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -brief -script "
            +basedir+"/testsubnested_run.pig;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExplainDot() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -dot -script "
            +basedir+"/testsubnested_run.pig;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testExplainOut() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "a = load 'foo' as (foo, fast, regenerate); explain -out /tmp -script "
            +basedir+"/testsubnested_run.pig;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testPartialExecution() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = "rmf bar; rmf baz; a = load 'file:test/org/apache/pig/test/data/passwd';"
            +"store a into 'bar'; exec; a = load 'bar'; store a into 'baz';\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testFileCmds() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "rmf bar; rmf baz;"
            +"a = load 'file:test/org/apache/pig/test/data/passwd';"
            +"store a into 'bar';"
            +"cp bar baz;"
            +"rm bar; rm baz;"
            +"store a into 'baz';"
            +"store a into 'bar';"
            +"rm baz; rm bar;"
            +"store a into 'baz';"
            +"mv baz bar;"
            +"b = load 'bar';"
            +"store b into 'baz';"
            +"cat baz;"
            +"rm baz;"
            +"rm bar;\n";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testCD() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "mkdir /tmp;"
            +"mkdir /tmp/foo;"
            +"cd /tmp;"
            +"rmf bar; rmf foo/baz;"
            +"copyFromLocal test/org/apache/pig/test/data/passwd bar;"
            +"a = load 'bar';"
            +"cd foo;"
            +"store a into 'baz';"
            +"cd /;"
            +"rm /tmp/bar; rm /tmp/foo/baz;";
        
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }
    
    @Test
    public void testDump() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "rmf bla;"
            +"a = load 'file:test/org/apache/pig/test/data/passwd';"
            +"e = group a by $0;"
            +"f = foreach e generate group, COUNT($1);"
            +"store f into 'bla';"
            +"f1 = load 'bla';"
            +"g = order f1 by $1;"
            +"dump g;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testIllustrate() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "rmf bla;"
            +"a = load 'file:test/org/apache/pig/test/data/passwd';"
            +"e = group a by $0;"
            +"f = foreach e generate group, COUNT($1);"
            +"store f into 'bla';"
            +"f1 = load 'bla' as (f:chararray);"
            +"g = order f1 by $1;"
            +"illustrate g;";

        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testKeepGoing() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "rmf bar;"
            +"rmf foo;"
            +"rmf baz;"
            +"A = load 'file:test/org/apache/pig/test/data/passwd';"
            +"B = foreach A generate 1;"
            +"C = foreach A generate 0/0;"
            +"store B into 'foo';"
            +"store C into 'bar';"
            +"A = load 'file:test/org/apache/pig/test/data/passwd';"
            +"B = stream A through `false`;"
            +"store B into 'baz';"
            +"cat bar;";
            
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);
    
        grunt.exec();
    }

    @Test
    public void testKeepGoigFailed() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "rmf bar;"
            +"rmf foo;"
            +"rmf baz;"
            +"A = load 'file:test/org/apache/pig/test/data/passwd';"
            +"B = foreach A generate 1;"
            +"C = foreach A generate 0/0;"
            +"store B into 'foo';"
            +"store C into 'bar';"
            +"A = load 'file:test/org/apache/pig/test/data/passwd';"
            +"B = stream A through `false`;"
            +"store B into 'baz';"
            +"cat baz;";
            
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (Exception e) {
            caught = true;
            assertTrue(e.getMessage().contains("baz does not exist"));
        }
        assertTrue(caught);
    }

    @Test
    public void testInvalidParam() throws Throwable {
        PigServer server = new PigServer(ExecType.LOCAL, cluster.getProperties());
        PigContext context = server.getPigContext();
        
        String strCmd = 
            "run -param -param;";
            
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (ParseException e) {
            caught = true;
            assertTrue(e.getMessage().contains("Encountered"));
        }
        assertTrue(caught);
    }

    @Test
    public void testStopOnFailure() throws Throwable {
        PigServer server = new PigServer(ExecType.MAPREDUCE, cluster.getProperties());
        PigContext context = server.getPigContext();
        context.getProperties().setProperty("stop.on.failure", ""+true);
        
        String strCmd = 
            "rmf bar;\n"
            +"rmf foo;\n"
            +"rmf baz;\n"
            +"copyFromLocal test/org/apache/pig/test/data/passwd pre;\n"
            +"A = load 'file:test/org/apache/pig/test/data/passwd';\n"
            +"B = stream A through `false`;\n"
            +"store B into 'bar' using BinStorage();\n"
            +"A = load 'bar';\n"
            +"store A into 'foo';\n"
            +"cp pre done;\n";
            
        ByteArrayInputStream cmd = new ByteArrayInputStream(strCmd.getBytes());
        InputStreamReader reader = new InputStreamReader(cmd);
        
        Grunt grunt = new Grunt(new BufferedReader(reader), context);

        boolean caught = false;
        try {
            grunt.exec();
        } catch (PigException e) {
            caught = true;
            assertTrue(e.getErrorCode() == 6017);
        }

        assertFalse(server.existsFile("done"));
        assertTrue(caught);
    }
}

