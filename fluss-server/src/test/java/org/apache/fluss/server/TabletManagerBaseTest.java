/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.fluss.server;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TabletManagerBase}. */
public class TabletManagerBaseTest {
    @Test
    void testGetPreferredDataDir() {
        Map<File, List<File>> allTabletDirs = new HashMap<>();
        allTabletDirs.put(
                new File("/data1"),
                Arrays.asList(new File("/data1/db1/tb1/log-1"), new File("/data1/db1/tb1/log-2")));
        allTabletDirs.put(new File("/data2"), List.of(new File("/data1/db1/tb1/log-3")));
        File dataDir = TabletManagerBase.getPreferredDataDir(allTabletDirs);
        assertThat(dataDir).isEqualTo(new File("/data2"));
    }
}
