/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aiven.inkless.engine;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Enumeration;

/**
 * A class loader for isolated Kafka plugin runtimes.
 *
 * <p>Plugin-private classes are loaded child-first, while Kafka platform classes are loaded
 * parent-first according to {@link KafkaPluginClassLoaderUtils#shouldLoadInIsolation(String)}.
 */
public class KafkaPluginClassLoader extends URLClassLoader {
    private static final String SERVICES_RESOURCE_PREFIX = "META-INF/services/";

    public KafkaPluginClassLoader(URL[] urls, ClassLoader parent) {
        super(urls, parent);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            Class<?> loaded = findLoadedClass(name);
            if (loaded == null) {
                // Missing optional implementations must stay absent. Falling back to a broker
                // copy can bind them to a different version of the plugin's private API.
                loaded = KafkaPluginClassLoaderUtils.shouldLoadInIsolation(name)
                    ? loadIsolated(name) : loadParentFirst(name);
            }
            if (resolve) {
                resolveClass(loaded);
            }
            return loaded;
        }
    }

    private Class<?> loadParentFirst(String name) throws ClassNotFoundException {
        try {
            return loadParentClass(name);
        } catch (ClassNotFoundException ignored) {
            return findClass(name);
        }
    }

    private Class<?> loadIsolated(String name) throws ClassNotFoundException {
        try {
            // Platform modules also expose packages such as org.w3c.dom and org.xml.sax.
            return ClassLoader.getPlatformClassLoader().loadClass(name);
        } catch (ClassNotFoundException ignored) {
            return findClass(name);
        }
    }

    private Class<?> loadParentClass(String name) throws ClassNotFoundException {
        ClassLoader parent = getParent();
        return parent == null ? findSystemClass(name) : parent.loadClass(name);
    }

    @Override
    public URL getResource(String name) {
        URL resource = findResource(name);
        return resource == null ? super.getResource(name) : resource;
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        if (name.startsWith(SERVICES_RESOURCE_PREFIX)) {
            String serviceClassName = name.substring(SERVICES_RESOURCE_PREFIX.length());
            if (!serviceClassName.isEmpty() && KafkaPluginClassLoaderUtils.shouldLoadInIsolation(serviceClassName)) {
                return findResources(name);
            }
        }
        return super.getResources(name);
    }
}
