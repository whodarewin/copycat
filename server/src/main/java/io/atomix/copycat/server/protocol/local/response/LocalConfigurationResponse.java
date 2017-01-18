/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License
 */
package io.atomix.copycat.server.protocol.local.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.local.response.AbstractLocalResponse;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.ConfigurationResponse;

import java.util.Collection;
import java.util.Objects;

/**
 * Server configuration response.
 * <p>
 * Configuration responses are sent in response to configuration change requests once a configuration
 * change is completed or fails. Note that configuration changes can frequently fail due to the limitation
 * of commitment of configuration changes. No two configuration changes may take place simultaneously. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class LocalConfigurationResponse extends AbstractLocalResponse implements ConfigurationResponse {
  protected final long index;
  protected final long term;
  protected final long timestamp;
  protected final Collection<Member> members;

  public LocalConfigurationResponse(Status status, CopycatError error, long index, long term, long timestamp, Collection<Member> members) {
    super(status, error);
    this.index = index;
    this.term = term;
    this.timestamp = timestamp;
    this.members = members;
  }

  /**
   * Returns the response index.
   *
   * @return The response index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the response configuration time.
   *
   * @return The response time.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the configuration members list.
   *
   * @return The configuration members list.
   */
  public Collection<Member> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, index, term, members);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isAssignableFrom(object.getClass())) {
      LocalConfigurationResponse response = (LocalConfigurationResponse) object;
      return response.status == status
        && response.index == index
        && response.term == term
        && response.timestamp == timestamp
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, index=%d, term=%d, timestamp=%d, members=%s]", getClass().getSimpleName(), status, index, term, timestamp, members);
  }

  /**
   * Configuration response builder.
   */
  public static abstract class Builder<T extends ConfigurationResponse.Builder<T, U>, U extends ConfigurationResponse> extends AbstractLocalResponse.Builder<T, U> implements ConfigurationResponse.Builder<T, U> {
    protected long index;
    protected long term;
    protected long timestamp;
    protected Collection<Member> members;

    @Override
    @SuppressWarnings("unchecked")
    public T withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index cannot be negative");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must be positive");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withTime(long time) {
      this.timestamp = Assert.argNot(time, time <= 0, "timestamp cannot be negative");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withMembers(Collection<Member> members) {
      this.members = Assert.notNull(members, "members");
      return (T) this;
    }
  }
}