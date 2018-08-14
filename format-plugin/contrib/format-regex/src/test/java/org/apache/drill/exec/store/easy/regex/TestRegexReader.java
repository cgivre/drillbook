/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.easy.regex;

import static org.junit.Assert.assertEquals;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.exec.record.BatchSchema;
import org.apache.drill.exec.record.metadata.TupleMetadata;
import org.apache.drill.exec.rpc.RpcException;
import org.apache.drill.exec.server.Drillbit;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.dfs.FileSystemConfig;
import org.apache.drill.exec.store.dfs.FileSystemPlugin;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterTest;
import org.apache.drill.test.LogFixture;
import org.apache.drill.test.LogFixture.LogFixtureBuilder;
import org.apache.drill.test.rowSet.RowSet;
import org.apache.drill.test.rowSet.RowSetBuilder;
import org.apache.drill.test.rowSet.RowSetComparison;
import org.apache.drill.test.rowSet.RowSetUtilities;
import org.apache.drill.test.rowSet.schema.SchemaBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import ch.qos.logback.classic.Level;

public class TestRegexReader extends ClusterTest {

  public static final String DATE_ONLY_PATTERN = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) .*";

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @BeforeClass
  public static void setup() throws Exception {
    ClusterTest.startCluster(ClusterFixture.builder(dirTestWatcher));

    // Define a regex format config for testing.

    defineRegexPlugin();
  }

  private static void defineRegexPlugin() throws ExecutionSetupException {

    // Create an instance of the regex config.
    // Note: we can't use the ".log" extension; the Drill .gitignore
    // file ignores such files, so they'll never get committed. Instead,
    // make up a fake suffix.

    final RegexFormatConfig sampleConfig = new RegexFormatConfig();
    sampleConfig.extension = "log1";
    sampleConfig.regex = DATE_ONLY_PATTERN;
    sampleConfig.fields = "year, month, day";

    // Full Drill log parser definition.

    final RegexFormatConfig logConfig = new RegexFormatConfig();
    logConfig.extension = "log2";
    logConfig.regex = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) " +
                      "(\\d\\d):(\\d\\d):(\\d\\d),\\d+ " +
                      "\\[([^]]*)] (\\w+)\\s+(\\S+) - (.*)";
    logConfig.fields = "year, month, day, hour, " +
        "minute, second, thread, level, module, message";

    // Define a temporary format plugin for the "cp" storage plugin.

    final Drillbit drillbit = cluster.drillbit();
    final StoragePluginRegistry pluginRegistry = drillbit.getContext().getStorage();
    final FileSystemPlugin plugin = (FileSystemPlugin) pluginRegistry.getPlugin("cp");
    final FileSystemConfig pluginConfig = (FileSystemConfig) plugin.getConfig();
    pluginConfig.getFormats().put("sample", sampleConfig);
    pluginConfig.getFormats().put("drill-log", logConfig);
    pluginRegistry.createOrUpdate("cp", pluginConfig, false);
  }

  @Test
  public void testWildcard() throws RpcException {
    final String sql = "SELECT * FROM cp.`regex/simple.log1`";
    final RowSet results = client.queryBuilder().sql(sql).rowSet();

    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("year", MinorType.VARCHAR)
        .addNullable("month", MinorType.VARCHAR)
        .addNullable("day", MinorType.VARCHAR)
        .build();

    final RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow("2017", "12", "17")
        .addRow("2017", "12", "18")
        .addRow("2017", "12", "19")
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testExplicit() throws RpcException {
    final String sql = "SELECT `day`, `missing`, `month` FROM cp.`regex/simple.log1`";
    final RowSet results = client.queryBuilder().sql(sql).rowSet();

    final BatchSchema expectedSchema = new SchemaBuilder()
        .addNullable("day", MinorType.VARCHAR)
        .addNullable("missing", MinorType.VARCHAR)
        .addNullable("month", MinorType.VARCHAR)
        .build();

    final RowSet expected = client.rowSetBuilder(expectedSchema)
        .addRow("17", null, "12")
        .addRow("18", null, "12")
        .addRow("19", null, "12")
        .build();

    RowSetUtilities.verify(expected, results);
  }

  @Test
  public void testCount() throws RpcException {
    final String sql = "SELECT COUNT(*) FROM cp.`regex/simple.log1`";
    final long result = client.queryBuilder().sql(sql).singletonLong();
    assertEquals(3, result);
  }

  @Test
  public void testFull() throws RpcException {
    final String sql = "SELECT * FROM cp.`regex/simple.log2`";
    client.queryBuilder().sql(sql).printCsv();
  }

  /**
   * Check handling of table functions, using the table function to
   * check handling of extra columns: more columns in the fields
   * list than groups in the regex.
   */

  @Test
  public void testExtraCols() throws RpcException {

    // Expose logging to the console. Verify manually that the output
    // includes a message like:
    //
    // Column list has more names than the pattern has groups,
    //   extras ignored. ...

    final LogFixtureBuilder logBuilder = LogFixture.builder()
        // Log to the console for debugging convenience
        .toConsole()
        // All debug messages in the regex package
        .logger("org.apache.drill.exec.store.easy.regex", Level.DEBUG);

    // Use the full pattern (keyed by ".log2", but with a short
    // pattern. The log should warn about unused columns.

    try (LogFixture logFixture = logBuilder.build()) {

      // Specify the pattern. Sorry for the \-explosion: both Java
      // and Drill want back-slashes escaped..
      // See DRILL-6167, DRILL-6168 and DRILL-6169

      final String sql = "SELECT * FROM table(cp.`regex/simple.log2`\n" +
          "(type => 'regex',\n" +
          " extension => 'log2',\n" +
          " regex => '(\\\\d\\\\d\\\\d\\\\d)-(\\\\d\\\\d)-(\\\\d\\\\d) .*',\n" +
          " fields => 'a, b, c, d'))";
      final RowSet results = client.queryBuilder().sql(sql).rowSet();

      final TupleMetadata schema = new SchemaBuilder()
          .addNullable("a", MinorType.VARCHAR)
          .addNullable("b", MinorType.VARCHAR)
          .addNullable("c", MinorType.VARCHAR)
          .buildSchema();
      final RowSet expected = new RowSetBuilder(client.allocator(), schema)
          .addRow("2017", "12", "17")
          .addRow("2017", "12", "18")
          .addRow("2017", "12", "19")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(results);
    }
  }

  @Test
  public void testMissingCols() throws RpcException {

    // Expose logging to the console. Verify manually that the output
    // includes a message like:
    //
    // Column list has fewer names than the pattern has groups,
    //    filling extras with Column$n ...

    final LogFixtureBuilder logBuilder = LogFixture.builder()
        // Log to the console for debugging convenience
        .toConsole()
        // All debug messages in the r package
        .logger("org.apache.drill.exec.store.easy.regex", Level.DEBUG);

    // Use the full pattern (keyed by ".log2", but with a short
    // pattern. The log should warn about missing columns.

    try (LogFixture logFixture = logBuilder.build()) {

      final String sql = "SELECT * FROM table(cp.`regex/simple.log2`\n" +
          "(type => 'regex',\n" +
          " extension => 'log2',\n" +
          " regex => '(\\\\d\\\\d\\\\d\\\\d)-(\\\\d\\\\d)-(\\\\d\\\\d) .*',\n" +
          " fields => 'a, b'))";
      final RowSet results = client.queryBuilder().sql(sql).rowSet();

      final TupleMetadata schema = new SchemaBuilder()
          .addNullable("a", MinorType.VARCHAR)
          .addNullable("b", MinorType.VARCHAR)
          .addNullable("Column$2", MinorType.VARCHAR)
          .buildSchema();
      final RowSet expected = new RowSetBuilder(client.allocator(), schema)
          .addRow("2017", "12", "17")
          .addRow("2017", "12", "18")
          .addRow("2017", "12", "19")
          .build();
      new RowSetComparison(expected)
        .verifyAndClearAll(results);
    }
  }

  @Test
  public void testNoCols() throws RpcException {

    final String sql = "SELECT * FROM table(cp.`regex/simple.log2`\n" +
        "(type => 'regex',\n" +
        " extension => 'log2',\n" +
        " regex => '(\\\\d\\\\d\\\\d\\\\d)-(\\\\d\\\\d)-(\\\\d\\\\d) .*'))";
    final RowSet results = client.queryBuilder().sql(sql).rowSet();
//    results.print();

    final TupleMetadata schema = new SchemaBuilder()
        .addNullable("Column$0", MinorType.VARCHAR)
        .addNullable("Column$1", MinorType.VARCHAR)
        .addNullable("Column$2", MinorType.VARCHAR)
        .buildSchema();
    final RowSet expected = new RowSetBuilder(client.allocator(), schema)
        .addRow("2017", "12", "17")
        .addRow("2017", "12", "18")
        .addRow("2017", "12", "19")
        .build();
    new RowSetComparison(expected)
      .verifyAndClearAll(results);
  }
}
