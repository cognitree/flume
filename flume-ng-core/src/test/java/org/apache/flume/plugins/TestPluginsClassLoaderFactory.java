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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.plugins;

import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.source.DefaultSourceFactory;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class TestPluginsClassLoaderFactory {

  private SourceFactory sourceFactory;
  private SinkFactory sinkFactory;

  @Before
  public void setUp() {
    String pluginsDir = "src/test/resources/plugins.d";
    PluginsClassLoaderFactory.initialize(pluginsDir);
    sourceFactory = new DefaultSourceFactory();
    sinkFactory = new DefaultSinkFactory();

  }

  @Test
  public void testSourceCreation() {
    final Source customSource1 = sourceFactory.create("customSource1", "org.apache.flume.source.CustomSourceOne");
    final Source customSource2 = sourceFactory.create("customSource2", "org.apache.flume.source.CustomSourceTwo");

    assertNotNull(customSource1);
    assertNotNull(customSource2);
    assertNotSame(customSource1, customSource2);

    assertNotSame(customSource1.getClass().getClassLoader(), customSource2.getClass().getClassLoader());
  }

  @Test
  public void testSinkCreation() {
    final Sink customSink1 = sinkFactory.create("customSink1", "org.apache.flume.sink.CustomSinkOne");
    final Sink customSink2 = sinkFactory.create("customSink2", "org.apache.flume.sink.CustomSinkTwo");

    assertNotNull(customSink1);
    assertNotNull(customSink2);
    assertNotSame(customSink1, customSink2);

    assertNotSame(customSink1.getClass().getClassLoader(), customSink2.getClass().getClassLoader());
  }
}
