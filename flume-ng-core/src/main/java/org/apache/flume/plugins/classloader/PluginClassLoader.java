package org.apache.flume.plugins.classloader;

import java.net.URL;
import java.net.URLClassLoader;

public class PluginClassLoader extends URLClassLoader {

  public PluginClassLoader(URL[] urls) {
     super(urls);
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    try {
      // First, check if the class has already been loaded
      Class<?> clazz = findLoadedClass(name);
      if(clazz == null) {
        clazz = findClass(name);
      }
      return clazz;
    } catch( ClassNotFoundException e ) {
      // didn't find it, try the parent
      return super.loadClass(name, resolve);
    }
  }
}