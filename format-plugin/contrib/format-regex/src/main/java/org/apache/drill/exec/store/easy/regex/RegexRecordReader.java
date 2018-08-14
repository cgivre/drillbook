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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;

/**
 * Parse a file using a regular expression. The regular expression must
 * have at least one pattern. The format can optionally include a list
 * of field names. Missing fields are filled in as "Column$n". Extra fields
 * are ignored.
 * <p>
 * Lines that do not match the regex are silently ignored.
 * <p>
 * The type of all columns is VarChar. If a column should be some other
 * type, that conversion can be done in a CAST in the query itself.
 * <p>
 * Projection is supported: no columns (for COUNT(*)), all columns (SELECT *)
 * or a list of columns. Projected columns not in the configured field list
 * are filled with nulls. Projected columns are matched to the field list
 * using standard SQL case insensitive comparison.
 */

public class RegexRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(RegexRecordReader.class);

  private static class ColumnDefn {
    private final String name;
    private final int index;
    private NullableVarCharVector.Mutator mutator;

    public ColumnDefn(String name, int index) {
      this.name = name;
      this.index = index;
    }
  }

  private static final int BATCH_SIZE = BaseValueVector.INITIAL_VALUE_ALLOCATION;

  private final DrillFileSystem dfs;
  private final FileWork fileWork;
  private final String userName;
  private final RegexFormatConfig formatConfig;
  private List<String> columnNames;
  private ColumnDefn columns[];
  private Pattern pattern;
  private int groupCount;
  private BufferedReader reader;
  private int rowIndex;

  public RegexRecordReader(FragmentContext context, DrillFileSystem dfs,
      FileWork fileWork, List<SchemaPath> columns, String userName,
      RegexFormatConfig formatConfig) {
    this.dfs = dfs;
    this.fileWork = fileWork;
    this.userName = userName;
    this.formatConfig = formatConfig;

    // Ask the superclass to parse the projection list.

    setColumns(columns);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) {
    setupPattern();
    setupColumns();
    setupProjection();
    openFile();
    defineVectors(output);
  }

  private void setupPattern() {

    // Compile the pattern

    try {
      pattern = Pattern.compile(formatConfig.getRegex());
    } catch (final PatternSyntaxException e) {
      throw UserException
          .validationError(e)
          .message("Failed to parse regex: \"%s\"", formatConfig.getRegex())
          .build(logger);
    }

    // Pattern must contain at least one group

    final Matcher matcher = pattern.matcher("");
    groupCount = matcher.groupCount();
    if (groupCount == 0) {
      throw UserException
        .validationError()
        .message("Regex contains no groups: \"%s\"", formatConfig.getRegex())
        .build(logger);
    }
  }

  private void setupColumns() {
    columnNames = new ArrayList<>();
    String fieldStr = formatConfig.getFields();
    if (fieldStr == null) {
      fieldStr = "";
    }

    // Ugly, but must work around DRILL-6169, table functions
    // cannot contain lists.

    final List<String> fields = Splitter.on(Pattern.compile("\\s*,\\s*")).splitToList(fieldStr);
    if (groupCount > fields.size()) {
      logger.warn(
          "Column list has fewer names than the pattern has groups, " +
          "filling extras with Column$n. " +
          "Column count {}, group count {}, columns: {}, regex: {}",
          fields.size(), groupCount,
          fields.toString(), formatConfig.getRegex());
    }
    else if (groupCount < fields.size()) {
      logger.warn(
          "Column list has more names than the pattern has groups, " +
          "extras ignored. Column count {}, group count {}, columns: {}, regex: {}",
          fields.size(), groupCount,
          fields.toString(), formatConfig.getRegex());
    }

    // Look for blank names. This will occur in cases such as:
    //
    // ""
    // ","
    // "a,,b"
    // etc.

    for (int i = 0; i < fields.size(); i++) {
      final String colName = fields.get(i);
      if (colName.isEmpty()) {
        columnNames.add(String.format("Column$%d", i));
      } else {
        columnNames.add(colName);
      }
    }
    for (int i = columnNames.size(); i < groupCount; i++) {
      columnNames.add(String.format("Column$%d", i));
    }
  }

  private void setupProjection() {
    if (isSkipQuery()) {
      projectNone();
    } else if (isStarQuery()) {
      projectAll();
    } else {
      projectSubset();
    }
  }

  private void projectNone() {
    columns = new ColumnDefn[] { new ColumnDefn("dummy", -1) };
  }

  private void projectAll() {
    columns = new ColumnDefn[groupCount];
    for (int i = 0; i < columns.length; i++) {
      columns[i] = new ColumnDefn(columnNames.get(i), i);
    }
  }

  private void projectSubset() {

    // Ensure the projected columns are only simple columns;
    // no maps, no arrays.

    final Collection<SchemaPath> project = this.getColumns();
    assert ! project.isEmpty();
    columns = new ColumnDefn[project.size()];
    int colIndex = 0;
    for (final SchemaPath column : project) {
      if (column.getAsNamePart().hasChild()) {
        throw UserException
            .validationError()
            .message("The regex format plugin supports only simple columns")
            .addContext("Projected column", column.toString())
            .build(logger);
      }

      // Find a matching defined column, case-insensitive match.

      final String name = column.getAsNamePart().getName();
      int patternIndex = -1;
      for (int i = 0; i < columnNames.size(); i++) {
        if (columnNames.get(i).equalsIgnoreCase(name)) {
          patternIndex = i;
          break;
        }
      }

      // Create the column. Index of -1 means column will be null.

      columns[colIndex++] = new ColumnDefn(name, patternIndex);
    }
  }

  private void openFile() {
    InputStream in;
    try {
      in = dfs.open(new Path(fileWork.getPath()));
    } catch (final Exception e) {
      throw UserException
        .dataReadError(e)
        .message("Failed to open open input file: %s", fileWork.getPath())
        .addContext("User name", userName)
        .build(logger);
    }
    reader = new BufferedReader(new InputStreamReader(in, Charsets.UTF_8));
  }

  private void defineVectors(OutputMutator output) {
    for (int i = 0; i < columns.length; i++) {
      final MaterializedField field = MaterializedField.create(columns[i].name,
          Types.optional(MinorType.VARCHAR));
      try {
        columns[i].mutator = output.addField(field, NullableVarCharVector.class).getMutator();
      } catch (final SchemaChangeException e) {
        throw UserException
          .systemError(e)
          .message("Vector creation failed")
          .build(logger);
      }
    }
  }

  @Override
  public int next() {
    rowIndex = 0;
    while (nextLine()) { }
    return rowIndex;
  }

  private boolean nextLine() {
    String line;
    try {
      line = reader.readLine();
    } catch (final IOException e) {
      throw UserException
        .dataReadError(e)
        .addContext("File", fileWork.getPath())
        .build(logger);
    }
    if (line == null) {
      return false;
    }
    final Matcher m = pattern.matcher(line);
    if (m.matches()) {
      loadVectors(m);
    }
    return rowIndex < BATCH_SIZE;
  }

  private void loadVectors(Matcher m) {

    // Core work: write values into vectors for the current
    // row. If projected by name, some columns may be null.

    for (int i = 0; i < columns.length; i++) {
      final NullableVarCharVector.Mutator mutator = columns[i].mutator;
      if (columns[i].index == -1) {
        // Not necessary; included just for clarity
        mutator.setNull(rowIndex);
      } else {
        final String value = m.group(columns[i].index + 1);
        if (value == null) {
          // Not necessary; included just for clarity
          mutator.setNull(rowIndex);
        } else {
          mutator.set(rowIndex, value.getBytes());
        }
      }
    }
    rowIndex++;
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (final IOException e) {
        logger.warn("Error when closing file: " + fileWork.getPath(), e);
      }
      reader = null;
    }
  }
}
