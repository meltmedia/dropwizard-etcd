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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * An etcd event representation for mapped directories.
 * 
 * @author Christian Trimble
 *
 * @param <T> the type of value being watched in etcd.
 */
public class EtcdEvent<T> {

  public static enum Type {
    added,
    removed,
    updated;
  }

  public static class Builder<T> {
    private T value;
    private T prevValue;
    private EtcdEvent.Type type;
    private String key;
    private Throwable cause;
    private Long index;

    public EtcdEvent.Builder<T> withValue( T value ) {
      this.value = value;
      return this;
    }
    
    public EtcdEvent.Builder<T> withPrevValue( T prevValue ) {
      this.prevValue = prevValue;
      return this;
    }
    
    public EtcdEvent.Builder<T> withType( EtcdEvent.Type type ) {
      this.type = type;
      return this;
    }
    
    public EtcdEvent.Builder<T> withKey( String key ) {
      this.key = key;
      return this;
    }
    
    public EtcdEvent.Builder<T> withIndex( Long index ) {
      this.index = index;
      return this;
    }
    
    public EtcdEvent.Builder<T> withCause( Throwable cause ) {
      this.cause = cause;
      return this;
    }
    
    public EtcdEvent<T> build() {
      return new EtcdEvent<T>(type, index, key, value, prevValue, cause);
    }
  }
  
  public static <T> EtcdEvent.Builder<T> builder() {
    return new EtcdEvent.Builder<T>();
  }

  private EtcdEvent.Type type;
  private T value;
  private T prevValue;
  private Throwable cause;
  private String key;
  private Long index;
  
  public EtcdEvent( EtcdEvent.Type type, Long index, String key, T value, T prevValue, Throwable cause ) {
    this.type = type;
    this.index = index;
    this.key = key;
    this.value = value;
    this.prevValue = prevValue;
    this.cause = cause;
  }
  
  public EtcdEvent.Type getType() { return this.type; }
  public String getKey() { return this.key; }
  public Long getIndex() { return this.index; }
  public T getValue() { return this.value; }
  public T getPrevValue() { return this.prevValue; }
  public Throwable getCause() { return this.cause; }

  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }
  
  public boolean equals( Object o ) {
    return EqualsBuilder.reflectionEquals(this, o);
  }
}