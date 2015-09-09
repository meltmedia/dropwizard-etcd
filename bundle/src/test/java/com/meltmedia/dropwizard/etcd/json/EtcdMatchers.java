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

import static org.mockito.Matchers.argThat;

import org.hamcrest.Description;
import org.mockito.ArgumentMatcher;

import com.google.common.base.Objects;
import com.meltmedia.dropwizard.etcd.json.EtcdEvent;

public class EtcdMatchers {

  public static <T> EtcdEvent<T> anyEtcdEventOfType(final EtcdEvent.Type type) {
    return argThat(new ArgumentMatcher<EtcdEvent<T>>() {
      public boolean matches(Object event) {
        return ((EtcdEvent<?>) event).getType() == type;
      }
    });
  }

  public static <T> EtcdEvent<T> atAnyIndex(final EtcdEvent<T> value) {
    return argThat(new ArgumentMatcher<EtcdEvent<T>>() {
      @SuppressWarnings("unchecked")
      public boolean matches(Object eventObject) {
        EtcdEvent<T> event = (EtcdEvent<T>) eventObject;
        return Objects.equal(event.getType(), value.getType())
          && Objects.equal(event.getKey(), value.getKey())
          && Objects.equal(event.getValue(), value.getValue())
          && Objects.equal(event.getPrevValue(), value.getPrevValue());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("expected " + value.getKey() + " without index");
      }
    });
  }
}
