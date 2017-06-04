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

import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.flume.plugins.classloader.PluginClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A class loader factory which isolates flume agent plugins to a class loader
 */
public class PluginsClassLoaderFactory {

  private static final Logger logger = LoggerFactory
      .getLogger(PluginsClassLoaderFactory.class);

  private static final List<ClassLoader> classLoaders = new ArrayList<>();
  private static final Map<String, ClassLoader> classLoadersMap = new HashMap<>();

  public static void initialize(String pluginsPath) {
    if(pluginsPath != null && !pluginsPath.isEmpty()) {
      FileFilter dirFilter = new NameFileFilter(new String[]{"lib", "libext"});
      for (String pluginsDir : pluginsPath.split("\\:")) {
        File pluginsRoot = new File(pluginsDir);
        if (pluginsRoot.isDirectory()) {
          final File[] plugins = pluginsRoot.listFiles();
          if (plugins != null) {
            for (File plugin : plugins) {
              if (plugin.isDirectory()) {
                final File[] files = plugin.listFiles(dirFilter);
                initializePluginClassLoader(files);
              }
            }
          }
        }
      }
    }
  }

  /**
   * creates a plugin class loader from all the jar file present in the list of directories
   */
  private static void initializePluginClassLoader(File[] files) {
    if (files != null) {
      FileFilter jarFileFilter = new SuffixFileFilter(".jar");
      ArrayList<URL> urls = new ArrayList<>();
      for (File file : files) {
        final File[] jarFiles = file.listFiles(jarFileFilter);
        if (jarFiles != null) {
          for (File jarFile : jarFiles) {
            try {
              urls.add(jarFile.toURI().toURL());
            } catch (MalformedURLException e) {
              logger.trace("Error adding jar file {} to class loader ",
                  jarFile.getAbsolutePath(), e);
            }
          }
        }
      }
      if (!urls.isEmpty()) {
        classLoaders.add(new PluginClassLoader(urls.toArray(new URL[urls.size()])));
      }
    }
  }

  public static Class getClass(String className) throws ClassNotFoundException {
    if (!classLoadersMap.containsKey(className)) {
      for (ClassLoader classLoader : classLoaders) {
        try {
          classLoader.loadClass(className);
          classLoadersMap.put(className, classLoader);
          break;
        } catch (ClassNotFoundException e) {
          // ignore and look in next classLoader for class with name className
          logger.trace("Unable to find class {} with class loader {}," +
              " moving on to next class loader", className, classLoader);
        }
      }
    }

    final ClassLoader classLoader = classLoadersMap.get(className);
    return classLoader == null ?
        Class.forName(className) : Class.forName(className, true, classLoader);
  }
}
