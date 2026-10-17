/* 2024-2025 */
package com.fronzec.frbatchservice.batchjobs.plugins;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fronzec.api.BatchJobPlugin;
import com.fronzec.frbatchservice.batchjobs.plugins.listener.JobExecutionAuditListener;
import com.fronzec.frbatchservice.batchjobs.plugins.repository.JobDefinitionRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.JobInstance;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.listener.JobExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.context.ApplicationContext;
import org.springframework.transaction.PlatformTransactionManager;

@ExtendWith(MockitoExtension.class)
class PluginRegistryServiceTest {

    @Mock private JobRegistry jobRegistry;
    @Mock private JobRepository jobRepository;
    @Mock private PlatformTransactionManager transactionManager;
    @Mock private ApplicationContext applicationContext;
    @Mock private JobDefinitionRepository jobDefinitionRepository;
    @Mock private JobExecutionAuditListener auditListener;

    private BatchJobPlugin stubPlugin(String jobName, Job producedJob) {
        BatchJobPlugin plugin = mock(BatchJobPlugin.class);
        when(plugin.getJobName()).thenReturn(jobName);
        when(plugin.configureJob(eq(jobRepository), eq(transactionManager), eq(applicationContext)))
                .thenReturn(producedJob);
        return plugin;
    }

    private Job stubJob(String name) {
        Job job = mock(Job.class);
        when(job.getName()).thenReturn(name);
        return job;
    }

    @Test
    void happyPath_singlePlugin_isDiscoveredAndRegistered() throws Exception {
        Job job = stubJob("job1");
        BatchJobPlugin plugin = stubPlugin("job1", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(plugin), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);
        service.registerAll();

        assertEquals(1, service.getPlugins().size());
        assertTrue(service.getRegisteredJobNames().contains("job1"));
        assertTrue(service.getPlugin("job1").isPresent());
    }

    @Test
    void duplicateJobName_throwsPluginRegistrationException_withBothClassNames() {
        Job job1 = stubJob("duplicateJob");

        BatchJobPlugin pluginA = mock(BatchJobPlugin.class);
        when(pluginA.getJobName()).thenReturn("duplicateJob");
        when(pluginA.configureJob(any(), any(), any())).thenReturn(job1);

        BatchJobPlugin pluginB = mock(BatchJobPlugin.class);
        when(pluginB.getJobName()).thenReturn("duplicateJob");
        // pluginB.configureJob() is NOT called — duplicate check fires first

        PluginRegistryService service = new PluginRegistryService(
                List.of(pluginA, pluginB), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        PluginRegistrationException ex = assertThrows(
                PluginRegistrationException.class, service::registerAll);

        String message = ex.getMessage();
        assertTrue(message.contains("duplicateJob"), "message should contain the conflicting job name");
        assertTrue(
                message.contains(pluginA.getClass().getName())
                        || message.contains(pluginB.getClass().getName()),
                "message should reference at least one plugin class");
    }

    @Test
    void configureJobThrows_wrapsInPluginRegistrationException_withPluginClassAndCause() {
        RuntimeException cause = new RuntimeException("simulated failure");

        BatchJobPlugin plugin = mock(BatchJobPlugin.class);
        when(plugin.getJobName()).thenReturn("failingJob");
        when(plugin.configureJob(any(), any(), any())).thenThrow(cause);

        PluginRegistryService service = new PluginRegistryService(
                List.of(plugin), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        PluginRegistrationException ex = assertThrows(
                PluginRegistrationException.class, service::registerAll);

        String message = ex.getMessage();
        assertTrue(
                message.contains(plugin.getClass().getName()),
                "message should contain the plugin class name");
        assertEquals(cause, ex.getCause(), "cause should be the original exception");
    }

    // ── Dynamic registration (PR 4) ────────────────────────────────────────

    @Test
    void registerDynamicPlugin_addsToMapAndJobRegistry() throws Exception {
        Job job = stubJob("dynamicJob");
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        service.registerDynamicPlugin(plugin);

        assertTrue(service.getRegisteredJobNames().contains("dynamicJob"));
        assertTrue(service.getPlugin("dynamicJob").isPresent());
        verify(jobRegistry).register(job);
    }

    @Test
    void registerDynamicPlugin_doesNotRetainClassloaderOwnership() throws Exception {
        Job job = stubJob("dynamicJob");
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        service.registerDynamicPlugin(plugin);

        assertEquals("CLASSPATH", service.getPluginBySource("dynamicJob"));
        assertThrows(
                NoSuchFieldException.class,
                () -> PluginRegistryService.class.getDeclaredField("classloaderRefs"));
    }

    @Test
    void registerDynamicPlugin_throwsOnDuplicate() throws Exception {
        Job job = stubJob("dynamicJob");
        BatchJobPlugin plugin1 = stubPlugin("dynamicJob", job);
        BatchJobPlugin plugin2 = mock(BatchJobPlugin.class);
        when(plugin2.getJobName()).thenReturn("dynamicJob");

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        service.registerDynamicPlugin(plugin1);

        PluginRegistrationException ex = assertThrows(
                PluginRegistrationException.class,
                () -> service.registerDynamicPlugin(plugin2));

        assertTrue(ex.getMessage().contains("dynamicJob"),
                "message should contain the conflicting job name");
    }

    @Test
    void unregisterDynamicPlugin_removesFromMapAndJobRegistry() throws Exception {
        Job job = stubJob("dynamicJob");
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        service.registerDynamicPlugin(plugin);
        service.unregisterDynamicPlugin("dynamicJob");

        assertFalse(service.getRegisteredJobNames().contains("dynamicJob"));
        assertTrue(service.getPlugin("dynamicJob").isEmpty());
        verify(jobRegistry).unregister("dynamicJob");
    }

    @Test
    void unregisterDynamicPlugin_propagatesRegistryFailureAndPreservesPluginMap() throws Exception {
        Job job = stubJob("dynamicJob");
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        service.registerDynamicPlugin(plugin);
        RuntimeException failure = new RuntimeException("registry unavailable");
        doThrow(failure).when(jobRegistry).unregister("dynamicJob");

        RuntimeException thrown =
                assertThrows(RuntimeException.class, () -> service.unregisterDynamicPlugin("dynamicJob"));

        assertEquals(failure, thrown);
        assertTrue(service.getRegisteredJobNames().contains("dynamicJob"));
        assertTrue(service.getPlugin("dynamicJob").isPresent());
    }

    @Test
    void unregisterDynamicPlugin_unknownName_logsWarning() throws Exception {
        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        // Should not throw — just logs a warning
        service.unregisterDynamicPlugin("nonexistent");

        // Verify jobRegistry.unregister was not called
        verify(jobRegistry, never()).unregister(any());
    }

    @Test
    void getPluginBySource_returnsNullForUnknownJob() throws Exception {
        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);

        assertNull(service.getPluginBySource("nonexistent"));
    }

    // ── Execution audit listener wiring (#111) ─────────────────────────────

    /** Builds a real {@link SimpleJob} that already carries a plugin-provided listener. */
    private SimpleJob realJobWithExistingListener(String name, JobExecutionListener existing)
            throws Exception {
        SimpleJob job = new SimpleJob(name);
        job.setJobRepository(jobRepository);
        job.registerJobExecutionListener(existing);
        job.afterPropertiesSet();
        return job;
    }

    /** Runs the job once and verifies both listeners saw the full lifecycle, existing one first. */
    private void assertBothListenersInvoked(SimpleJob job, JobExecutionListener existing)
            throws Exception {
        JobExecution execution =
                new JobExecution(1L, new JobInstance(1L, job.getName()), new JobParameters());
        job.execute(execution);

        InOrder order = inOrder(existing, auditListener);
        order.verify(existing).beforeJob(execution);
        order.verify(auditListener).beforeJob(execution);
        verify(existing).afterJob(execution);
        verify(auditListener).afterJob(execution);
    }

    @Test
    void registerAll_attachesAuditListener_andKeepsExistingListeners() throws Exception {
        JobExecutionListener existing = mock(JobExecutionListener.class);
        SimpleJob job = realJobWithExistingListener("job1", existing);
        BatchJobPlugin plugin = stubPlugin("job1", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(plugin), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);
        service.registerAll();

        verify(jobRegistry).register(job);
        assertBothListenersInvoked(job, existing);
    }

    @Test
    void registerDynamicPlugin_attachesAuditListener_andKeepsExistingListeners() throws Exception {
        JobExecutionListener existing = mock(JobExecutionListener.class);
        SimpleJob job = realJobWithExistingListener("dynamicJob", existing);
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);
        service.registerDynamicPlugin(plugin);

        verify(jobRegistry).register(job);
        assertBothListenersInvoked(job, existing);
    }

    @Test
    void registerDynamicPlugin_reRegisteringTheSameJobInstance_attachesAuditListenerOnce()
            throws Exception {
        JobExecutionListener existing = mock(JobExecutionListener.class);
        SimpleJob job = realJobWithExistingListener("dynamicJob", existing);
        BatchJobPlugin plugin = stubPlugin("dynamicJob", job);

        PluginRegistryService service = new PluginRegistryService(
                List.of(), jobRegistry, jobRepository, transactionManager,
                applicationContext, jobDefinitionRepository, auditListener);
        service.registerDynamicPlugin(plugin);
        service.unregisterDynamicPlugin("dynamicJob");
        service.registerDynamicPlugin(plugin);

        // verify(...) defaults to times(1): a duplicate attachment would fire beforeJob twice
        assertBothListenersInvoked(job, existing);
    }

    @Test
    void registerDynamicPlugin_nonAbstractJob_registersUnauditedWithWarning() throws Exception {
        Job job = stubJob("customJob");
        BatchJobPlugin plugin = stubPlugin("customJob", job);
        Logger logger = (Logger) LoggerFactory.getLogger(PluginRegistryService.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);

        try {
            PluginRegistryService service = new PluginRegistryService(
                    List.of(), jobRegistry, jobRepository, transactionManager,
                    applicationContext, jobDefinitionRepository, auditListener);
            service.registerDynamicPlugin(plugin);

            verify(jobRegistry).register(job);
            assertTrue(service.getRegisteredJobNames().contains("customJob"));
            assertTrue(
                    appender.list.stream()
                            .anyMatch(event -> event.getLevel() == Level.WARN
                                    && event.getFormattedMessage().contains("customJob")
                                    && event.getFormattedMessage().contains(job.getClass().getName())),
                    "a WARN naming the job and its class should be logged");
        } finally {
            logger.detachAppender(appender);
        }
    }
}
