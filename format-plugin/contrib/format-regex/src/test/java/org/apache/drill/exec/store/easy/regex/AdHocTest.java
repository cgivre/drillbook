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

import static org.junit.Assert.assertTrue;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.drill.exec.ExecConstants;
import org.apache.drill.test.BaseDirTestWatcher;
import org.apache.drill.test.ClientFixture;
import org.apache.drill.test.ClusterFixture;
import org.apache.drill.test.ClusterFixtureBuilder;
import org.apache.drill.test.DrillTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class AdHocTest extends DrillTest {

  @ClassRule
  public static final BaseDirTestWatcher dirTestWatcher = new BaseDirTestWatcher();

  @Test
  public void runServer() throws Exception {
    final ClusterFixtureBuilder builder = ClusterFixture.builder(dirTestWatcher);
    builder.withRemoteZk("localhost:2181");
    builder.configProperty(ExecConstants.HTTP_ENABLE, true);
    try (ClusterFixture cluster = builder.build();
         ClientFixture client = cluster.clientFixture()) {
      Thread.sleep(24 * 60 * 60 * 1000);
    }
  }

  @Test
  public void testRegex() {
    final String line = "2017-12-31 20:52:42,045 [main] ERROR o.apache.drill.exec.server.Drillbit - Failure during initial startup of Drillbit.";
    final String regex = "(\\d\\d\\d\\d)-(\\d\\d)-(\\d\\d) (\\d\\d):(\\d\\d):(\\d\\d),\\d+ \\[([^]]*)] (\\w+) (\\S+) - (.*)";
    final Pattern p = Pattern.compile(regex);
    final Matcher m = p.matcher(line);
    assertTrue(m.matches());
  }

}
