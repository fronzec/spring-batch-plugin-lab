/* 2024-2025 */
package com.fronzec.frbatchservice.batchjobs;

import com.fronzec.api.BatchJobPlugin;
import com.fronzec.frbatchservice.batchjobs.plugins.PluginRegistryService;
import com.fronzec.frbatchservice.batchjobs.plugins.metrics.PluginMetrics;
import com.fronzec.frbatchservice.utils.JsonUtils;
import com.fronzec.frbatchservice.web.SingleJobDataRequest;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.parameters.InvalidJobParametersException;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.launch.JobRestartException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.stereotype.Service;

/** Manages the lifecycle of batch jobs, delegating discovery and registry to {@link PluginRegistryService}. */
@Service
public class JobsManagerService {

    private static final Logger logger = LoggerFactory.getLogger(JobsManagerService.class);

    private final JobOperator asyncJobLauncher;
    private final JobOperator syncJobLauncher;
    private final JobRegistry jobRegistry;
    private final PluginRegistryService pluginRegistryService;
    private final JobRepository jobRepository;
    private final PluginMetrics pluginMetrics;

    public JobsManagerService(
            JobOperator asyncJobLauncher,
            JobOperator syncJobLauncher,
            JobRegistry jobRegistry,
            PluginRegistryService pluginRegistryService,
            JobRepository jobRepository,
            PluginMetrics pluginMetrics) {
        this.asyncJobLauncher = asyncJobLauncher;
        this.syncJobLauncher = syncJobLauncher;
        this.jobRegistry = jobRegistry;
        this.pluginRegistryService = pluginRegistryService;
        this.jobRepository = jobRepository;
        this.pluginMetrics = pluginMetrics;
    }

    public HashMap<String, String> launchAllNonAutoDetectedJobsAsync(
            LocalDate date, final int runningNumber) {
        return launchAllNonAutoDetectedJobs(date, runningNumber, false);
    }

    public HashMap<String, String> launchAllNonAutoDetectedJobsSync(
            LocalDate execDate, final int runningNumber) {
        return launchAllNonAutoDetectedJobs(execDate, runningNumber, true);
    }

    /**
     * Launches all registered plugin jobs using the provided execution date and attempt number.
     *
     * <p>Each job receives parameters from {@link BatchJobPlugin#getDefaultParameters()} overlaid by
     * the caller-supplied {@code DATE} and {@code ATTEMPT_NUMBER} values. Caller-supplied values win.
     *
     * @param execDate        the execution date to add as the "DATE" job parameter (must not be null)
     * @param runningNumber   the attempt number to add as the "ATTEMPT_NUMBER" job parameter
     * @param useSyncExecutor when true, use the synchronous job launcher; when false, use the asynchronous launcher
     * @return a map from job name to either the job's identifier string on successful launch or an error message
     */
    private HashMap<String, String> launchAllNonAutoDetectedJobs(
            LocalDate execDate, final int runningNumber, boolean useSyncExecutor) {
        Objects.requireNonNull(execDate);
        HashMap<String, String> resultInfoHolder = new HashMap<>();

        for (BatchJobPlugin plugin : pluginRegistryService.getPlugins()) {
            String key = plugin.getJobName();

            Job theJob = jobRegistry.getJob(key);
            if (theJob == null) {
                logger.warn("Job '{}' not found in registry — skipping", key);
                resultInfoHolder.put(key, String.format("Job <%s> not found", key));
                continue;
            }

            // Start from plugin defaults, then overlay caller-supplied identifying params (caller wins)
            var jobParams = new HashMap<>(plugin.getDefaultParameters());
            jobParams.put("DATE", execDate.toString());
            jobParams.put("ATTEMPT_NUMBER", String.valueOf(runningNumber));

            var jobParametersBuilder = new JobParametersBuilder();
            jobParams.forEach(jobParametersBuilder::addString);
            // Non-identifying descriptor
            jobParametersBuilder.addString("DESCRIPTION", "some useful description", false);

            try {
                logger.info("Running job: {} with params -> {}", theJob, jobParams);
                if (useSyncExecutor) {
                    syncJobLauncher.run(theJob, jobParametersBuilder.toJobParameters());
                } else {
                    asyncJobLauncher.run(theJob, jobParametersBuilder.toJobParameters());
                }
                resultInfoHolder.put(key, theJob.toString());
            } catch (JobExecutionAlreadyRunningException e) {
                logger.error("already", e);
                resultInfoHolder.put(key, e.getMessage());
            } catch (JobRestartException e) {
                logger.error("restart", e);
                resultInfoHolder.put(key, e.getMessage());
            } catch (JobInstanceAlreadyCompleteException e) {
                logger.error("completed", e);
                resultInfoHolder.put(key, e.getMessage());
            } catch (InvalidJobParametersException e) {
                logger.info("parameters", e);
                resultInfoHolder.put(key, e.getMessage());
            }
        }

        return resultInfoHolder;
    }

    /**
     * Launches a registered plugin job asynchronously using parameters from the request.
     *
     * @param request container holding the target job bean name and a map of string parameters;
     *                must include "DATE" (ISO-8601 date); "ATTEMPT_NUMBER" defaults to "1" if absent
     * @return a map containing outcome details with keys "jobName", "result", or "error"
     */
    public Map<String, String> asyncRunJobWithParams(SingleJobDataRequest request) {
        Map<String, String> launchedJobMetadata = new HashMap<>();
        Map<String, String> jobExecParams = new HashMap<>();

        Job theJob = jobRegistry.getJob(request.getJobBeanName());
        if (theJob == null) {
            launchedJobMetadata.put(
                    "error", String.format("Job <%s> not found", request.getJobBeanName()));
            return launchedJobMetadata;
        }

        try {
            launchedJobMetadata.put("jobName", request.getJobBeanName());
            var jobParametersBuilder = new JobParametersBuilder();

            String dateStr = request.getParams().get("DATE");
            if (dateStr == null || dateStr.isBlank()) {
                launchedJobMetadata.put("error", "Missing required parameter: DATE");
                return launchedJobMetadata;
            }
            LocalDate execDate = LocalDate.parse(dateStr);
            jobExecParams.put("DATE", execDate.toString());
            String attemptStr = request.getParams().get("ATTEMPT_NUMBER");
            jobExecParams.put("ATTEMPT_NUMBER", attemptStr != null ? attemptStr : "1");

            jobExecParams.forEach(jobParametersBuilder::addString);
            jobParametersBuilder.addString(
                    "DESCRIPTION", request.getParams().getOrDefault("DESCRIPTION", ""), false);
            logger.info(
                    "Attempt to run job -> {} with params -> {}",
                    theJob,
                    JsonUtils.parseObject2Json(jobExecParams));
            asyncJobLauncher.run(theJob, jobParametersBuilder.toJobParameters());
            launchedJobMetadata.put("result", JsonUtils.parseObject2Json(theJob.toString()));
        } catch (JobExecutionAlreadyRunningException e) {
            logger.error("already running job with the same params", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (JobRestartException e) {
            logger.error("restart", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (JobInstanceAlreadyCompleteException e) {
            logger.error("completed", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (InvalidJobParametersException e) {
            logger.info("parameters", e);
            launchedJobMetadata.put("error", e.getMessage());
        }

        return launchedJobMetadata;
    }

    /**
     * Runs the specified registered plugin job synchronously using parameters supplied in the request.
     *
     * @param request a SingleJobDataRequest whose params map must contain:
     *                - "DATE": ISO-8601 date string used as the job's {@code DATE} parameter
     *                - "ATTEMPT_NUMBER": an integer string used as the job's {@code ATTEMPT_NUMBER} parameter (defaults to "1" if absent)
     *                - "description": a non-identifying description added as {@code DESCRIPTION}
     * @return a map of result keys: "jobName", "parameters", "result" on success; "error" on failure or not-found
     */
    public Map<String, String> syncRunJobWithParams(SingleJobDataRequest request) {
        Map<String, String> launchedJobMetadata = new HashMap<>();
        Map<String, String> jobExecParams = new HashMap<>();

        Job theJob = jobRegistry.getJob(request.getJobBeanName());
        if (theJob == null) {
            launchedJobMetadata.put(
                    "error", String.format("Job <%s> not found", request.getJobBeanName()));
            return launchedJobMetadata;
        }

        try {
            launchedJobMetadata.put("jobName", request.getJobBeanName());
            pluginMetrics.incrementExecuted(request.getJobBeanName());
            var jobParametersBuilder = new JobParametersBuilder();

            String dateStr = request.getParams().get("DATE");
            if (dateStr == null || dateStr.isBlank()) {
                launchedJobMetadata.put("error", "Missing required parameter: DATE");
                return launchedJobMetadata;
            }
            LocalDate execDate = LocalDate.parse(dateStr);
            jobExecParams.put("DATE", execDate.toString());
            String attemptStr = request.getParams().get("ATTEMPT_NUMBER");
            jobExecParams.put("ATTEMPT_NUMBER", attemptStr != null ? attemptStr : "1");

            jobExecParams.forEach(jobParametersBuilder::addString);
            jobParametersBuilder.addString(
                    "DESCRIPTION", request.getParams().getOrDefault("DESCRIPTION", ""), false);
            logger.info(
                    "Attempt to run job -> {} with identifying params -> {}",
                    theJob,
                    JsonUtils.parseObject2Json(jobExecParams));
            JobExecution run = syncJobLauncher.run(theJob, jobParametersBuilder.toJobParameters());
            launchedJobMetadata.put(
                    "parameters",
                    JsonUtils.parseObject2Json(jobParametersBuilder.toJobParameters()));
            launchedJobMetadata.put("result", run.getExitStatus().toString());
        } catch (JobExecutionAlreadyRunningException e) {
            logger.error("already running job with the same params", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (JobRestartException e) {
            logger.error("restart", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (JobInstanceAlreadyCompleteException e) {
            logger.error("completed", e);
            launchedJobMetadata.put("error", e.getMessage());
        } catch (InvalidJobParametersException e) {
            logger.info("parameters", e);
            launchedJobMetadata.put("error", e.getMessage());
        }

        return launchedJobMetadata;
    }

    public HashMap<String, String> stopAllJobs() {
        HashMap<String, String> signaledStopped = new HashMap<>();
        pluginRegistryService
                .getRegisteredJobNames()
                .forEach(
                        name -> {
                            Set<JobExecution> runningExecutions =
                                    jobRepository.findRunningJobExecutions(name);
                            logger.info(
                                    "Stopping job name -> {} :: running -> {} ",
                                    name,
                                    runningExecutions);
                            runningExecutions.forEach(
                                    exId -> {
                                        try {
                                            syncJobLauncher.stop(exId);
                                        } catch (JobExecutionNotRunningException e) {
                                            logger.error("no running", e);
                                        }
                                    });
                            signaledStopped.put(name, runningExecutions.toString());
                        });

        return signaledStopped;
    }

    public HashMap<String, Map<String, String>> getRunning() {
        HashMap<String, Map<String, String>> running = new HashMap<>();
        pluginRegistryService
                .getRegisteredJobNames()
                .forEach(
                        key -> {
                            HashMap<String, String> tmp = new HashMap<>();
                            Set<JobExecution> runningExecutions =
                                    jobRepository.findRunningJobExecutions(key);
                            runningExecutions.forEach(
                                    r -> tmp.put(
                                            String.valueOf(r.getId()),
                                            r.getJobParameters().toString()));
                            running.put(key, tmp);
                        });
        return running;
    }
}
