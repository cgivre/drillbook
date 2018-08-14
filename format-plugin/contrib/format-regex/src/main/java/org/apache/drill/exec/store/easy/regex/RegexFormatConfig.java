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

import java.util.Arrays;
import java.util.Objects;

import org.apache.drill.common.logical.FormatPluginConfig;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Format for the regex plugin.
 * <p>
 * The list of fields is encoded as a string to work around DRILL-6169.
 * Example:<pre><code>
 * col1, col2, col3
 * </code></pre>
 * The list may have any number of spaces (including none) before or after
 * the comma.
 * <p>
 * When used in a table function, DRILL-6167 forces us to specify the type
 * as follows:<pre><code>
 * SELECT * FROM table(cp.`regex/simple.log2`
 *   (type => 'regex', regex => 'some pattern'))
 * </code></pre>
 */

@JsonTypeName("regex")
@JsonInclude(Include.NON_DEFAULT)
public class RegexFormatConfig implements FormatPluginConfig {

  // Note: fields in this class are public due to DRILL-6672.
  // If the fields were private, could not make them final, and
  // provide a constructor, due to DRILL-6673.

  public String regex;

  // Should be a List<String>. But, table functions don't support
  // lists, so we store the fields as single string that contains
  // a comma-delimited list: a, b, c. Spaces are optional.
  // See DRILL-6169.

  public String fields;
  public String extension;

  public String getRegex() { return regex; }
  public String getFields() { return fields; }
  public String getExtension() { return extension; }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) { return true; }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final RegexFormatConfig other = (RegexFormatConfig) obj;
    return Objects.equals(regex, other.regex) &&
           Objects.equals(fields, other.fields) &&
           Objects.equals(extension, other.extension);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(new Object[] {regex, fields, extension});
  }
}
