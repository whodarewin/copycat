/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.PooledDirectAllocator;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.io.File;
import java.time.Duration;

/**
 * Immutable log configuration/factory.
 * <p>
 * This class provides a factory for {@link Log} objects. {@code Storage} objects are immutable and
 * can be created only via the {@link Storage.Builder}. To create a new
 * {@code Storage.Builder}, use the static {@link #builder()} factory method:
 * <pre>
 *   {@code
 *     Storage storage = Storage.builder()
 *       .withDirectory(new File("logs"))
 *       .withPersistenceLevel(PersistenceLevel.DISK)
 *       .build();
 *   }
 * </pre>
 * Users can also configure a number of options related to how {@link Log logs} are constructed and managed.
 * Most notable of the configuration options is the number of {@link #compactionThreads()}, which specifies the
 * number of background threads to use to clean log {@link Segment segments}. The parallelism of the log
 * compaction algorithm will be limited by the number of {@link #compactionThreads()}.
 *
 * @see Log
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Storage {
  private static final String DEFAULT_DIRECTORY = System.getProperty("user.dir");
  private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 8;
  private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
  private static final int DEFAULT_MAX_ENTRIES_PER_SEGMENT = 1024 * 1024;
  private static final int DEFAULT_COMPACTION_THREADS = Runtime.getRuntime().availableProcessors() / 2;
  private static final Duration DEFAULT_MINOR_COMPACTION_INTERVAL = Duration.ofMinutes(1);
  private static final Duration DEFAULT_MAJOR_COMPACTION_INTERVAL = Duration.ofMinutes(10);
  private static final double DEFAULT_COMPACTION_THRESHOLD = 0.5;

  private StorageLevel storageLevel = StorageLevel.DISK;
  private Serializer serializer = new Serializer(new PooledDirectAllocator());
  private File directory = new File(DEFAULT_DIRECTORY);
  private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
  private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
  private int maxEntriesPerSegment = DEFAULT_MAX_ENTRIES_PER_SEGMENT;
  private int compactionThreads = DEFAULT_COMPACTION_THREADS;
  private Duration minorCompactionInterval = DEFAULT_MINOR_COMPACTION_INTERVAL;
  private Duration majorCompactionInterval = DEFAULT_MAJOR_COMPACTION_INTERVAL;
  private double compactionThreshold = DEFAULT_COMPACTION_THRESHOLD;

  public Storage() {
  }

  public Storage(StorageLevel storageLevel) {
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(String directory) {
    this(new File(Assert.notNull(directory, "directory")));
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(File directory) {
    this(directory, StorageLevel.DISK);
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(Serializer serializer) {
    this(StorageLevel.DISK, serializer);
  }

  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(String directory, Serializer serializer) {
    this(new File(Assert.notNull(directory, "directory")), serializer);
  }

  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(File directory, Serializer serializer) {
    this(directory, StorageLevel.DISK, serializer);
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(String directory, StorageLevel storageLevel) {
    this(new File(Assert.notNull(directory, "directory")), storageLevel);
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(File directory, StorageLevel storageLevel) {
    this.directory = Assert.notNull(directory, "directory");
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
  }

  /**
   * @throws NullPointerException if {@code directory} is null
   */
  public Storage(StorageLevel storageLevel, Serializer serializer) {
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
    this.serializer = Assert.notNull(serializer, "serializer");
  }

  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(String directory, StorageLevel storageLevel, Serializer serializer) {
    this(new File(Assert.notNull(directory, "directory")), storageLevel, serializer);
  }

  /**
   * @throws NullPointerException if {@code directory} or {@code serializer} are null
   */
  public Storage(File directory, StorageLevel storageLevel, Serializer serializer) {
    this.directory = Assert.notNull(directory, "directory");
    this.storageLevel = Assert.notNull(storageLevel, "storageLevel");
    this.serializer = Assert.notNull(serializer, "serializer");
  }

  /**
   * Returns a new storage builder.
   *
   * @return A new storage builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the storage serializer.
   *
   * @return The storage serializer.
   */
  public Serializer serializer() {
    return serializer;
  }

  /**
   * Returns the storage directory.
   *
   * @return The storage directory.
   */
  public File directory() {
    return directory;
  }

  /**
   * Returns the storage level.
   *
   * @return The storage level.
   */
  public StorageLevel level() {
    return storageLevel;
  }

  /**
   * Returns the maximum storage entry size.
   *
   * @return The maximum entry size in bytes.
   */
  public int maxEntrySize() {
    return maxEntrySize;
  }

  /**
   * Returns the maximum storage segment size.
   *
   * @return The maximum segment size in bytes.
   */
  public int maxSegmentSize() {
    return maxSegmentSize;
  }

  /**
   * Returns the maximum number of entries per segment.
   *
   * @return The maximum number of entries per segment.
   */
  public int maxEntriesPerSegment() {
    return maxEntriesPerSegment;
  }

  /**
   * Returns the number of log compaction threads.
   *
   * @return The number of log compaction threads.
   */
  public int compactionThreads() {
    return compactionThreads;
  }

  /**
   * Returns the minor compaction interval.
   *
   * @return The minor compaction interval.
   */
  public Duration minorCompactionInterval() {
    return minorCompactionInterval;
  }

  /**
   * Returns the major compaction interval.
   *
   * @return The major compaction interval.
   */
  public Duration majorCompactionInterval() {
    return majorCompactionInterval;
  }

  /**
   * Returns the compaction threshold.
   *
   * @return The compaction threshold.
   */
  public double compactionThreshold() {
    return compactionThreshold;
  }

  /**
   * Opens the underlying log.
   *
   * @return The opened log.
   */
  public Log open(String name) {
    return new Log(name, this);
  }

  @Override
  public String toString() {
    return String.format("%s[directory=%s]", getClass().getSimpleName(), directory);
  }

  /**
   * Storage builder.
   */
  public static class Builder extends io.atomix.catalyst.util.Builder<Storage> {
    private final Storage storage = new Storage();

    private Builder() {
    }

    /**
     * Sets the log storage level.
     *
     * @param storageLevel The log storage level.
     * @return The storage builder.
     */
    public Builder withStorageLevel(StorageLevel storageLevel) {
      storage.storageLevel = Assert.notNull(storageLevel, "storageLevel");
      return this;
    }

    /**
     * Sets the log entry serializer.
     *
     * @param serializer The log entry serializer.
     * @return The storage builder.
     * @throws NullPointerException If the serializer is {@code null}
     */
    public Builder withSerializer(Serializer serializer) {
      storage.serializer = Assert.notNull(serializer, "serializer");
      return this;
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(String directory) {
      return withDirectory(new File(Assert.notNull(directory, "directory")));
    }

    /**
     * Sets the log directory, returning the builder for method chaining.
     * <p>
     * The log will write segment files into the provided directory. It is recommended that a unique directory be dedicated
     * for each unique log instance.
     *
     * @param directory The log directory.
     * @return The storage builder.
     * @throws NullPointerException If the {@code directory} is {@code null}
     */
    public Builder withDirectory(File directory) {
      storage.directory = Assert.notNull(directory, "directory");
      return this;
    }

    /**
     * Sets the maximum entry count, returning the builder for method chaining.
     * <p>
     * The maximum entry count will be used to place an upper limit on the count of log segments.
     *
     * @param maxEntrySize The maximum entry count.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxEntrySize} is not positive or {@code maxEntrySize} is not
     * less than the max segment size.
     */
    public Builder withMaxEntrySize(int maxEntrySize) {
      Assert.arg(maxEntrySize > 0, "maximum entry size must be positive");
      Assert.argNot(maxEntrySize > storage.maxSegmentSize, "maximum entry size must be less than maxSegmentSize");
      storage.maxEntrySize = maxEntrySize;
      return this;
    }

    /**
     * Sets the maximum segment count, returning the builder for method chaining.
     *
     * @param maxSegmentSize The maximum segment count.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive or {@code maxSegmentSize} 
     * is not greater than the maxEntrySize
     */
    public Builder withMaxSegmentSize(int maxSegmentSize) {
      Assert.arg(maxSegmentSize > 0, "maxSegmentSize must be positive");
      Assert.argNot(maxSegmentSize < storage.maxEntrySize, "maximum segment size must be greater than maxEntrySize");
      storage.maxSegmentSize = maxSegmentSize;
      return this;
    }

    /**
     * Sets the maximum number of allows entries per segment.
     *
     * @param maxEntriesPerSegment The maximum number of entries allowed per segment.
     * @return The storage builder.
     * @throws IllegalArgumentException If the {@code maxEntriesPerSegment} not greater than the default max entries per
     * segment
     */
    public Builder withMaxEntriesPerSegment(int maxEntriesPerSegment) {
      Assert.argNot(maxEntriesPerSegment > DEFAULT_MAX_ENTRIES_PER_SEGMENT,
          "max entries per segment cannot be greater than " + DEFAULT_MAX_ENTRIES_PER_SEGMENT);
      storage.maxEntriesPerSegment = maxEntriesPerSegment;
      return this;
    }

    /**
     * Sets the number of log compaction threads.
     *
     * @param compactionThreads The number of log compaction threads.
     * @return The storage builder.
     * @throws IllegalArgumentException if {@code compactionThreads} is not positive
     */
    public Builder withCompactionThreads(int compactionThreads) {
      storage.compactionThreads = Assert.arg(compactionThreads, compactionThreads > 0, "compactionThreads must be positive");
      return this;
    }

    /**
     * Sets the minor compaction interval.
     *
     * @param interval The minor compaction interval.
     * @return The storage builder.
     */
    public Builder withMinorCompactionInterval(Duration interval) {
      storage.minorCompactionInterval = Assert.notNull(interval, "interval");
      return this;
    }

    /**
     * Sets the major compaction interval.
     *
     * @param interval The major compaction interval.
     * @return The storage builder.
     */
    public Builder withMajorCompactionInterval(Duration interval) {
      storage.majorCompactionInterval = Assert.notNull(interval, "interval");
      return this;
    }

    /**
     * Sets the percentage of entries in the segment that must be cleaned before a segment can be compacted.
     *
     * @param threshold The segment compact threshold.
     * @return The storage builder.
     */
    public Builder withCompactionThreshold(double threshold) {
      storage.compactionThreshold = Assert.argNot(threshold, threshold <= 0, "threshold must be positive");
      return this;
    }

    @Override
    public Storage build() {
      return storage;
    }
  }

}
