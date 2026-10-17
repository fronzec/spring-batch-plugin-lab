/* 2024-2025 */
package com.fronzec.frbatchservice.batchjobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fronzec.api.BatchJobPlugin;
import com.fronzec.frbatchservice.batchjobs.plugins.PluginRegistryService;
import com.fronzec.frbatchservice.batchjobs.plugins.metrics.PluginMetrics;
import com.fronzec.frbatchservice.web.SingleJobDataRequest;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.launch.JobOperator;

@ExtendWith(MockitoExtension.class)
public class JobsManagerServiceTest {

    private JobsManagerService jobsManagerService;

    @Mock private JobOperator jobOperator;
    @Mock private JobRegistry jobRegistry;
    @Mock private PluginRegistryService pluginRegistryService;
    @Mock private PluginMetrics pluginMetrics;
    @Mock private BatchJobPlugin plugin;
    @Mock private Job job;

    @BeforeEach
    void setUp() {
        jobsManagerService =
                new JobsManagerService(
                    jobOperator, jobOperator, jobRegistry, pluginRegistryService, null,
                    pluginMetrics);
    }

    @Test
    void testLaunchAllPlugins() throws Exception {
        LocalDate date = LocalDate.now();
        int runningNumber = 1;

        when(plugin.getJobName()).thenReturn("TestJob");
        when(plugin.getDefaultParameters()).thenReturn(Map.of("paramname", "paramvalue"));
        when(pluginRegistryService.getPlugins()).thenReturn((Collection) List.of(plugin));
        when(jobRegistry.getJob("TestJob")).thenReturn(job);

        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString("DATE", date.toString());
        jobParametersBuilder.addString("ATTEMPT_NUMBER", String.valueOf(runningNumber));

        when(jobOperator.run(any(Job.class), any(JobParameters.class)))
                .thenReturn(
                        new JobExecution(
                                1L,
                                new JobInstance(1L, "TestJob"),
                                jobParametersBuilder.toJobParameters()));

        HashMap<String, String> result =
                jobsManagerService.launchAllNonAutoDetectedJobsAsync(date, runningNumber);

        verify(jobOperator).run(any(Job.class), any(JobParameters.class));
        assertEquals(1, result.size());
        assertTrue(result.containsKey("TestJob"));
        assertNotNull(result.get("TestJob"));
    }

    @Test
    void testLaunchAllPluginsWithException() throws Exception {
        LocalDate date = LocalDate.now();
        int runningNumber = 1;

        when(plugin.getJobName()).thenReturn("TestJob");
        when(plugin.getDefaultParameters()).thenReturn(Map.of());
        when(pluginRegistryService.getPlugins()).thenReturn((Collection) List.of(plugin));
        when(jobRegistry.getJob("TestJob")).thenReturn(job);
        when(jobOperator.run(any(Job.class), any(JobParameters.class)))
                .thenThrow(new JobExecutionAlreadyRunningException("Job is already running"));

        HashMap<String, String> result =
                jobsManagerService.launchAllNonAutoDetectedJobsAsync(date, runningNumber);

        verify(jobOperator).run(any(Job.class), any(JobParameters.class));
        assertEquals(1, result.size());
        assertEquals("Job is already running", result.get("TestJob"));
    }

    @Test
    void testSyncRunJobWithParams_jobNotFound_returnsErrorMap() {
        when(jobRegistry.getJob("missingJob")).thenReturn(null);

        SingleJobDataRequest request = new SingleJobDataRequest();
        request.setJobBeanName("missingJob");

        Map<String, String> result = jobsManagerService.syncRunJobWithParams(request);

        assertTrue(result.containsKey("error"));
        assertTrue(result.get("error").contains("missingJob"));
        assertFalse(result.containsKey("result"));
    }

    @Test
    void testAsyncRunJobWithParams_jobNotFound_returnsErrorMap() {
        when(jobRegistry.getJob("unknownJob")).thenReturn(null);

        SingleJobDataRequest request = new SingleJobDataRequest();
        request.setJobBeanName("unknownJob");

        Map<String, String> result = jobsManagerService.asyncRunJobWithParams(request);

        assertTrue(result.containsKey("error"));
        assertTrue(result.get("error").contains("unknownJob"));
        assertFalse(result.containsKey("result"));
    }
}
