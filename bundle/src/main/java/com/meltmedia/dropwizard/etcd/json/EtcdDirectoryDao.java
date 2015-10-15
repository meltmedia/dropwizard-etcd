/**
 * Copyright (C) 2015 meltmedia (christian.trimble@meltmedia.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.meltmedia.dropwizard.etcd.json;

import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import mousio.etcd4j.EtcdClient;
import mousio.etcd4j.responses.EtcdException;
import mousio.etcd4j.responses.EtcdKeysResponse;

/**
 * A dao for Etcd directories where all values in the directory are of a common type.
 * 
 * @author Christian Trimble
 *
 * @param <T> the type of the values in this directory.
 */
public class EtcdDirectoryDao<T> {

  ObjectMapper mapper;
  Supplier<EtcdClient> clientSupplier;
  TypeReference<T> type;
  String directory;

  public EtcdDirectoryDao(Supplier<EtcdClient> clientSupplier, String directory,
    ObjectMapper mapper, TypeReference<T> type) {
    this.clientSupplier = clientSupplier;
    this.mapper = mapper;
    this.type = type;
    this.directory = directory;
  }

  /**
   * Puts a value into the directory and returns the etcd index of the value.  The key should not start with the '/' character.
   * 
   * @param key
   * @param entry
   * @return
   */
  public Long put(String key, T entry) {
    try {
      return clientSupplier.get().put(directory + "/" + key, mapper.writeValueAsString(entry))
        .send().get().node.modifiedIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to put key %s", key), e);
    }
  }

  /**
   * Puts a value into the directory, with a time to live in seconds and returns the etcd index of the value.  The key should
   * not start with the '/' character.
   * 
   * @param key
   * @param entry
   * @param ttl
   * @return
   */
  public Long putWithTtl(String key, T entry, Integer ttl) {
    try {
      return clientSupplier.get().put(directory + "/" + key, mapper.writeValueAsString(entry))
        .ttl(ttl).send().get().etcdIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to put key %s", key), e);
    }
  }

  /**
   * Updates the time to live of a given key and value and returns the new etcd index of that value.
   * 
   * @param key
   * @param entry
   * @param ttl
   * @return
   */
  public Long update(String key, T entry, Integer ttl) {
    try {
      String value = mapper.writeValueAsString(entry);
      return clientSupplier.get().put(directory + "/" + key, value).ttl(ttl).prevValue(value)
        .send().get().etcdIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to update ttl on key %s", key), e);
    }
  }

  /**
   * Removes the specified key from this directory.
   * 
   * @param key
   * @return
   */
  public Long remove(String key) {
    try {
      return clientSupplier.get().delete(directory + "/" + key).send().get().etcdIndex;
    } catch (EtcdException e) {
      if (e.errorCode == 100) {
        return e.index.longValue();
      }
      throw new EtcdDirectoryException(String.format("failed to delete key %s", key), e);
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to delete key %s", key), e);
    }
  }

  /**
   * Deletes this directory and all of its children.  A new, empty directory is then created with
   * the same name and the etcd index of that directory is returned.
   * 
   * @return
   */
  public Long resetDirectory() {
    try {
      clientSupplier.get().deleteDir(directory).recursive().send().get();
    } catch (Exception e) {
    }
    try {
      return clientSupplier.get().putDir(directory).isDir().send().get().etcdIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to reset directory %s", directory), e);
    }
  }

  /**
   * Returns a stream of the values in this directory.
   * 
   * @return a stream of the values in this directory.
   */
  public Stream<T> stream() {
    try {
      EtcdKeysResponse response = clientSupplier.get().getDir(directory).send().get();

      if (response.node == null || response.node.nodes == null) {
        return Stream.empty();
      }

      return response.node.nodes.stream().map(n -> {
        try {
          return mapper.readValue(n.value, type);
        } catch (Exception e) {
          return null;
        }
      });
    } catch (Exception e) {
      if (e instanceof EtcdException && ((EtcdException) e).errorCode == 100)
        return Stream.empty();
      throw new EtcdDirectoryException(String.format("failed to list directory %s", directory), e);
    }
  }

  /**
   * Gets the value for the specified key.
   * 
   * @param key
   * @return
   */
  public T get(String key) {
    try {
      return mapper.readValue(
        clientSupplier.get().get(directory + "/" + key).send().get().node.value, type);
    } catch (EtcdException e) {
      if (e.errorCode == 100) {
        throw new KeyNotFound(e.etcdMessage, e);
      }
      throw new EtcdDirectoryException(e.etcdMessage, e);
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("could not load key %s from directory %s",
        key, directory), e);
    }
  }

  /**
   * Puts a new directory to the specified key.
   * 
   * @param key
   * @return
   */
  public Long putDir(String key) {
    try {
      return clientSupplier.get().putDir(directory + key).send().get().node.modifiedIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to put directory key %s", key), e);
    }
  }

  /**
   * Updates the value of key, using the specified transform, if its current value matches predicate.
   * 
   * @param key
   * @param precondition
   * @param transform
   * @return
   */
  public Long update(String key, Predicate<T> precondition, Function<T, T> transform) {
    T currentValue = get(key);
    if (!precondition.test(currentValue)) {
      throw new EtcdDirectoryException(String.format("precondition failed while updating %s", key));
    }
    T nextValue = transform.apply(clone(currentValue));
    return put(key, nextValue, currentValue);
  }

  /**
   * Helper method to clone objects of this directory's type.
   * 
   * @param value
   * @return
   */
  public T clone(T value) {
    return mapper.convertValue(mapper.convertValue(value, JsonNode.class), type);
  }

  /**
   * Puts the given value into key, if the previous value matches.
   */
  public Long put(String key, T value, T previousValue) {
    try {
      return clientSupplier.get().put(directory + "/" + key, mapper.writeValueAsString(value))
        .prevValue(mapper.writeValueAsString(previousValue)).send().get().node.modifiedIndex;
    } catch (Exception e) {
      throw new EtcdDirectoryException(String.format("failed to put key %s with previous value",
        key), e);
    }
  }

}
