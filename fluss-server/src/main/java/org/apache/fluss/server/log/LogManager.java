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

package org.apache.fluss.server.log;

import org.apache.fluss.annotation.VisibleForTesting;
import org.apache.fluss.config.ConfigOptions;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.exception.FlussRuntimeException;
import org.apache.fluss.exception.LogStorageException;
import org.apache.fluss.exception.SchemaNotExistException;
import org.apache.fluss.metadata.LogFormat;
import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TableInfo;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.server.TabletManagerBase;
import org.apache.fluss.server.log.checkpoint.OffsetCheckpointFile;
import org.apache.fluss.server.metrics.group.TabletServerMetricGroup;
import org.apache.fluss.server.zk.ZooKeeperClient;
import org.apache.fluss.utils.FileUtils;
import org.apache.fluss.utils.FlussPaths;
import org.apache.fluss.utils.MapUtils;
import org.apache.fluss.utils.clock.Clock;
import org.apache.fluss.utils.concurrent.Scheduler;
import org.apache.fluss.utils.types.Tuple2;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static org.apache.fluss.utils.concurrent.LockUtils.inLock;

/* This file is based on source code of Apache Kafka Project (https://kafka.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/**
 * The entry point to the fluss log management subsystem. The log manager is responsible for log
 * creation, retrieval, and cleaning. All read and write operations are delegated to the individual
 * log instances.
 */
@ThreadSafe
public final class LogManager extends TabletManagerBase {
    private static final Logger LOG = LoggerFactory.getLogger(LogManager.class);

    @VisibleForTesting
    static final String RECOVERY_POINT_CHECKPOINT_FILE = "recovery-point-offset-checkpoint";

    /**
     * Clean shutdown file that indicates the tabletServer was cleanly shutdown in v0.7 and higher.
     * This is used to avoid unnecessary recovery operations after a clean shutdown like recovery
     * writer snapshot by scan all logs during loadLogs.
     *
     * <p>Note: for the previous cluster deploy by v0.6 and lower, there's no this file, we default
     * think its unclean shutdown.
     */
    static final String CLEAN_SHUTDOWN_FILE = ".fluss_cleanshutdown";

    private final ZooKeeperClient zkClient;
    private final Scheduler scheduler;
    private final Clock clock;
    private final TabletServerMetricGroup serverMetricGroup;
    private final ReentrantLock logCreationOrDeletionLock = new ReentrantLock();

    private final Map<TableBucket, LogTablet> currentLogs = MapUtils.newConcurrentHashMap();

    private volatile OffsetCheckpointFile recoveryPointCheckpoint;
    private boolean loadLogsCompletedFlag = false;

    private final File checkpointDir;
    private final File cleanShutdownDir;

    private LogManager(
            Configuration conf,
            ZooKeeperClient zkClient,
            int recoveryThreadsPerDataDir,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup)
            throws Exception {
        super(TabletType.LOG, conf, recoveryThreadsPerDataDir);
        this.zkClient = zkClient;
        this.scheduler = scheduler;
        this.clock = clock;
        this.serverMetricGroup = serverMetricGroup;
        createAndValidateDataDirs();

        checkpointDir = dataDirs.get(0);
        initializeCheckpointMaps();
        cleanShutdownDir = dataDirs.get(0);
    }

    public static LogManager create(
            Configuration conf,
            ZooKeeperClient zkClient,
            Scheduler scheduler,
            Clock clock,
            TabletServerMetricGroup serverMetricGroup)
            throws Exception {
        return new LogManager(
                conf,
                zkClient,
                conf.getInt(ConfigOptions.NETTY_SERVER_NUM_WORKER_THREADS),
                scheduler,
                clock,
                serverMetricGroup);
    }

    public void startup() {
        loadLogs();

        // TODO add more scheduler, like log-flusher etc.
    }

    public File getCheckpointDir() {
        return checkpointDir;
    }

    private void initializeCheckpointMaps() throws IOException {
        recoveryPointCheckpoint =
                new OffsetCheckpointFile(new File(checkpointDir, RECOVERY_POINT_CHECKPOINT_FILE));
    }

    /** Recover and load all logs in the given data directories. */
    private void loadLogs() {
        LOG.info("Loading logs from data directories {}", dataDirs);

        try {
            boolean isCleanShutdown = false;
            File cleanShutdownFile = new File(cleanShutdownDir, CLEAN_SHUTDOWN_FILE);
            if (cleanShutdownFile.exists()) {
                // Cache the clean shutdown status marker and use that for rest of log loading
                // workflow. Delete the CleanShutdownFile so that if tabletServer crashes while
                // loading the log, it is considered hard shutdown during the next boot up.
                Files.deleteIfExists(cleanShutdownFile.toPath());
                isCleanShutdown = true;
            }

            Map<TableBucket, Long> recoveryPoints = new HashMap<>();
            try {
                recoveryPoints = recoveryPointCheckpoint.read();
            } catch (Exception e) {
                LOG.warn(
                        "Error occurred while reading recovery-point-offset-checkpoint file of directory {}, "
                                + "resetting the recovery checkpoint to 0",
                        checkpointDir.getAbsolutePath(),
                        e);
            }

            List<File> tabletsToLoad = listTabletsToLoad();
            List<File> logTabletsToLoad =
                    tabletsToLoad.stream()
                            .filter(
                                    tabletDir ->
                                            tabletDir
                                                    .getName()
                                                    .startsWith(FlussPaths.LOG_TABLET_DIR_PREFIX))
                            .collect(Collectors.toList());
            if (logTabletsToLoad.isEmpty()) {
                LOG.info("No logs found to be loaded in {}", dataDirs);
            } else if (isCleanShutdown) {
                LOG.info("Skipping some recovery log process since clean shutdown file was found");
            } else {
                LOG.info("Recovering all local logs since no clean shutdown file was not found");
            }

            final Map<TableBucket, Long> finalRecoveryPoints = recoveryPoints;
            final boolean cleanShutdown = isCleanShutdown;
            // set runnable job.
            Runnable[] jobsForDir =
                    createLogLoadingJobs(
                            logTabletsToLoad, cleanShutdown, finalRecoveryPoints, conf, clock);

            long startTime = System.currentTimeMillis();

            int successLoadCount = runInThreadPool(jobsForDir, "log-recovery");

            loadLogsCompletedFlag = true;
            LOG.info(
                    "log loader complete. Total success loaded log count is {}, Take {} ms",
                    successLoadCount,
                    System.currentTimeMillis() - startTime);
        } catch (Throwable e) {
            throw new FlussRuntimeException("Failed to recovery log", e);
        }
    }

    /**
     * Get or create log tablet for a given bucket of a table. If the log already exists, just
     * return a copy of the existing log. Otherwise, create a log for the given table and the given
     * bucket.
     *
     * @param tablePath the table path of the bucket belongs to
     * @param tableBucket the table bucket
     * @param logFormat the log format
     * @param tieredLogLocalSegments the number of segments to retain in local for tiered log
     * @param isChangelog whether the log is a changelog of primary key table
     */
    public LogTablet getOrCreateLog(
            PhysicalTablePath tablePath,
            TableBucket tableBucket,
            LogFormat logFormat,
            int tieredLogLocalSegments,
            boolean isChangelog)
            throws Exception {
        return inLock(
                logCreationOrDeletionLock,
                () -> {
                    if (currentLogs.containsKey(tableBucket)) {
                        return currentLogs.get(tableBucket);
                    }

                    File tabletDir = getOrCreateTabletDir(tablePath, tableBucket);

                    LogTablet logTablet =
                            LogTablet.create(
                                    tablePath,
                                    tabletDir,
                                    conf,
                                    serverMetricGroup,
                                    0L,
                                    scheduler,
                                    logFormat,
                                    tieredLogLocalSegments,
                                    isChangelog,
                                    clock,
                                    true);
                    currentLogs.put(tableBucket, logTablet);

                    LOG.info(
                            "Loaded log for bucket {} in dir {}",
                            tableBucket,
                            tabletDir.getAbsolutePath());

                    return logTablet;
                });
    }

    public Optional<LogTablet> getLog(TableBucket tableBucket) {
        return Optional.ofNullable(currentLogs.get(tableBucket));
    }

    public void dropLog(TableBucket tableBucket) {
        LogTablet dropLogTablet =
                inLock(logCreationOrDeletionLock, () -> currentLogs.remove(tableBucket));

        if (dropLogTablet != null) {
            TablePath tablePath = dropLogTablet.getTablePath();
            try {
                dropLogTablet.drop();
                if (dropLogTablet.getPartitionName() == null) {
                    LOG.info(
                            "Deleted log bucket {} for table {} in file path {}.",
                            tableBucket.getBucket(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                } else {
                    LOG.info(
                            "Deleted log bucket {} for the partition {} of table {} in file path {}.",
                            tableBucket.getBucket(),
                            dropLogTablet.getPartitionName(),
                            tablePath,
                            dropLogTablet.getLogDir().getAbsolutePath());
                }
            } catch (Exception e) {
                throw new LogStorageException(
                        String.format(
                                "Error while deleting log for table %s, bucket %s in dir %s: %s",
                                tablePath,
                                tableBucket.getBucket(),
                                dropLogTablet.getLogDir().getAbsolutePath(),
                                e.getMessage()),
                        e);
            }
        } else {
            throw new LogStorageException(
                    String.format(
                            "Failed to delete log bucket %s as it does not exist.",
                            tableBucket.getBucket()));
        }
    }

    /**
     * Truncate the bucket's logs to the specified offsets and checkpoint the recovery point to this
     * offset.
     */
    public void truncateTo(TableBucket tableBucket, long offset) throws LogStorageException {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null && logTablet.truncateTo(offset)) {
            checkpointRecoveryOffsets();
        }
    }

    public void truncateFullyAndStartAt(TableBucket tableBucket, long newOffset) {
        LogTablet logTablet = currentLogs.get(tableBucket);
        // If the log tablet does not exist, skip it.
        if (logTablet != null) {
            logTablet.truncateFullyAndStartAt(newOffset);
            checkpointRecoveryOffsets();
        }
    }

    private LogTablet loadLog(
            File tabletDir,
            boolean isCleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock)
            throws Exception {
        Tuple2<PhysicalTablePath, TableBucket> pathAndBucket = FlussPaths.parseTabletDir(tabletDir);
        TableBucket tableBucket = pathAndBucket.f1;
        long logRecoveryPoint = recoveryPoints.getOrDefault(tableBucket, 0L);

        PhysicalTablePath physicalTablePath = pathAndBucket.f0;
        TablePath tablePath = physicalTablePath.getTablePath();
        TableInfo tableInfo = getTableInfo(zkClient, tablePath);
        LogTablet logTablet =
                LogTablet.create(
                        physicalTablePath,
                        tabletDir,
                        conf,
                        serverMetricGroup,
                        logRecoveryPoint,
                        scheduler,
                        tableInfo.getTableConfig().getLogFormat(),
                        tableInfo.getTableConfig().getTieredLogLocalSegments(),
                        tableInfo.hasPrimaryKey(),
                        clock,
                        isCleanShutdown);

        if (currentLogs.containsKey(tableBucket)) {
            throw new IllegalStateException(
                    String.format(
                            "Duplicate log tablet directories for bucket %s are found in both %s and %s. "
                                    + "It is likely because tablet directory failure happened while server was "
                                    + "replacing current replica with future replica. Recover server from this "
                                    + "failure by manually deleting one of the two log directories for this bucket. "
                                    + "It is recommended to delete the bucket in the log tablet directory that is "
                                    + "known to have failed recently.",
                            tableBucket,
                            tabletDir.getAbsolutePath(),
                            currentLogs.get(tableBucket).getLogDir().getAbsolutePath()));
        }
        currentLogs.put(tableBucket, logTablet);

        return logTablet;
    }

    private void createAndValidateDataDir(File dataDir) {
        try {
            if (!dataDir.exists()) {
                LOG.info("Data directory {} not found, creating it.", dataDir.getAbsolutePath());
                boolean created = dataDir.mkdirs();
                if (!created) {
                    throw new IOException(
                            "Failed to create data directory " + dataDir.getAbsolutePath());
                }
                Path parentPath = dataDir.toPath().toAbsolutePath().normalize().getParent();
                FileUtils.flushDir(parentPath);
            }
            if (!dataDir.isDirectory() || !dataDir.canRead()) {
                throw new IOException(
                        dataDir.getAbsolutePath() + " is not a readable data directory.");
            }
        } catch (IOException e) {
            throw new FlussRuntimeException(
                    "Failed to create or validate data directory " + dataDir.getAbsolutePath(), e);
        }
    }

    private void createAndValidateDataDirs() {
        inLock(
                logCreationOrDeletionLock,
                () -> {
                    for (File dataDir : dataDirs) {
                        createAndValidateDataDir(dataDir);
                    }
                });
    }

    /** Close all the logs. */
    public void shutdown() {
        LOG.info("Shutting down LogManager.");

        ExecutorService pool = createThreadPool("log-tablet-closing");

        List<LogTablet> logs = new ArrayList<>(currentLogs.values());
        List<Future<?>> jobsForTabletDir = new ArrayList<>();
        for (LogTablet logTablet : logs) {
            Runnable runnable =
                    () -> {
                        try {
                            logTablet.flush(true);
                            logTablet.close();
                        } catch (IOException e) {
                            throw new FlussRuntimeException(e);
                        }
                    };
            jobsForTabletDir.add(pool.submit(runnable));
        }

        try {
            for (Future<?> future : jobsForTabletDir) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    LOG.warn("Interrupted while shutting down LogManager.");
                } catch (ExecutionException e) {
                    LOG.warn(
                            "There was an error in one of the threads during LogManager shutdown",
                            e);
                }
            }

            // update the last flush point.
            checkpointRecoveryOffsets();

            // mark that the shutdown was clean by creating marker file for log dirs that all logs
            // have been recovered at startup time.
            if (loadLogsCompletedFlag) {
                LOG.debug("Writing clean shutdown marker.");
                try {
                    Files.createFile(new File(cleanShutdownDir, CLEAN_SHUTDOWN_FILE).toPath());
                } catch (IOException e) {
                    LOG.warn("Failed to write clean shutdown marker.", e);
                }
            }
        } finally {
            pool.shutdown();
        }

        LOG.info("Shut down LogManager complete.");
    }

    private Map<Tuple2<PhysicalTablePath, TableBucket>, Tuple2<File, File>> groupByTableBucket(
            List<File> tabletDirs) {
        Map<Tuple2<PhysicalTablePath, TableBucket>, Tuple2<File, File>> tableBucketTabletTuples =
                new HashMap<>();
        for (File tabletDir : tabletDirs) {
            Tuple2<PhysicalTablePath, TableBucket> tableBucket =
                    FlussPaths.parseTabletDir(tabletDir);
            Tuple2<File, File> tabletTuple = tableBucketTabletTuples.get(tableBucket);
            if (tabletDir.getName().startsWith(FlussPaths.LOG_TABLET_DIR_PREFIX)) {
                if (tabletTuple == null) {
                    tabletTuple = Tuple2.of(tabletDir, null);
                    tableBucketTabletTuples.put(tableBucket, tabletTuple);
                } else {
                    tabletTuple.f0 = tabletDir;
                }
            } else if (tabletDir.getName().startsWith(FlussPaths.KV_TABLET_DIR_PREFIX)) {
                if (tabletTuple == null) {
                    tabletTuple = Tuple2.of(null, tabletDir);
                    tableBucketTabletTuples.put(tableBucket, tabletTuple);
                } else {
                    tabletTuple.f1 = tabletDir;
                }
            } else {
                throw new IllegalArgumentException("Invalid tablet directory " + tabletDir);
            }
        }
        return tableBucketTabletTuples;
    }

    /** Create runnable jobs for loading logs from tablet directories. */
    private Runnable[] createLogLoadingJobs(
            List<File> tabletsToLoad,
            boolean cleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock) {
        Map<Tuple2<PhysicalTablePath, TableBucket>, Tuple2<File, File>> tableBucketTabletTuples =
                groupByTableBucket(tabletsToLoad);
        List<Tuple2<File, File>> tabletDirTuples =
                new ArrayList<>(tableBucketTabletTuples.values());
        Runnable[] jobs = new Runnable[tabletDirTuples.size()];
        for (int i = 0; i < tabletDirTuples.size(); i++) {
            final Tuple2<File, File> tabletDirTuple = tabletDirTuples.get(i);
            jobs[i] =
                    createLogLoadingJob(
                            tabletDirTuple.f0,
                            tabletDirTuple.f1,
                            cleanShutdown,
                            recoveryPoints,
                            conf,
                            clock);
        }
        return jobs;
    }

    /** Create a runnable job for loading log from a single tablet directory. */
    private Runnable createLogLoadingJob(
            File logTabletDir,
            File kvTabletDir,
            boolean cleanShutdown,
            Map<TableBucket, Long> recoveryPoints,
            Configuration conf,
            Clock clock) {
        return new Runnable() {
            @Override
            public void run() {
                LOG.debug("Loading log {}", logTabletDir);
                try {
                    loadLog(logTabletDir, cleanShutdown, recoveryPoints, conf, clock);
                } catch (Exception e) {
                    LOG.error("Fail to loadLog from {}", logTabletDir, e);
                    if (e instanceof SchemaNotExistException) {
                        LOG.error(
                                "schema not exist, table for {} has already been dropped, the residual data will be removed.",
                                logTabletDir,
                                e);
                        FileUtils.deleteDirectoryQuietly(logTabletDir);

                        // Also delete corresponding KV tablet directory if it exists
                        try {
                            if (kvTabletDir.exists()) {
                                LOG.info(
                                        "Also removing corresponding KV tablet directory: {}",
                                        kvTabletDir);
                                FileUtils.deleteDirectoryQuietly(kvTabletDir);
                            }
                        } catch (Exception kvDeleteException) {
                            LOG.warn(
                                    "Failed to delete corresponding KV tablet directory {} for log {}: {}",
                                    kvTabletDir,
                                    logTabletDir,
                                    kvDeleteException.getMessage());
                        }
                        return;
                    }
                    throw new FlussRuntimeException(e);
                }
            }
        };
    }

    @VisibleForTesting
    void checkpointRecoveryOffsets() {
        // Assuming TableBucket and LogTablet are actual types used in your application
        if (recoveryPointCheckpoint != null) {
            try {
                Map<TableBucket, Long> recoveryOffsets = new HashMap<>();
                for (Map.Entry<TableBucket, LogTablet> entry : currentLogs.entrySet()) {
                    recoveryOffsets.put(entry.getKey(), entry.getValue().getRecoveryPoint());
                }
                recoveryPointCheckpoint.write(recoveryOffsets);
            } catch (Exception e) {
                throw new LogStorageException(
                        "Disk error while writing recovery offsets checkpoint in directory "
                                + checkpointDir
                                + ": "
                                + e.getMessage(),
                        e);
            }
        }
    }
}
