package com.pluralsight.springbatch.patientbatchloader.config;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.explore.support.JobExplorerFactoryBean;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Custom {@link BatchConfigurer} implementation to control the configuration of {@link JobRepository}, {@link
 * JobExplorer} and {@link JobLauncher}
 *
 * @author alvaro.pedraza
 */
@Component
@EnableBatchProcessing // Triggers Sp Batch to include and configure feats as beans
public class BatchConfiguration implements BatchConfigurer {

    private JobRepository jobRepository; // persists meta-data about batch jobs
    private JobExplorer jobExplorer; // retrieves meta-data from the job repository (provides getters)
    private JobLauncher jobLauncher; // run jobs with given parameters

    // Driver for the datasource configuration override
    @Autowired
    @Qualifier("batchTransactionManager")
    private PlatformTransactionManager batchTransactionManager;

    @Autowired
    @Qualifier("batchDataSource")
    private DataSource batchDataSource;

    @Override
    public JobRepository getJobRepository() {
        return this.jobRepository;
    }

    @Override
    public PlatformTransactionManager getTransactionManager() {
        return this.batchTransactionManager;
    }

    @Override
    public JobLauncher getJobLauncher() {
        return this.jobLauncher;
    }

    @Override
    public JobExplorer getJobExplorer() {
        return this.jobExplorer;
    }

    // Handles the actual bean configuration
    @PostConstruct
    public void afterPropertiesSet() throws Exception {
        this.jobRepository = createJobRepository();
        this.jobExplorer = createJobExplorer();
        this.jobLauncher = createJobLauncher();
    }

    private JobRepository createJobRepository() throws Exception {
        JobRepositoryFactoryBean factory = new JobRepositoryFactoryBean();
        factory.setDataSource(this.batchDataSource);
        factory.setTransactionManager(getTransactionManager());
        factory.afterPropertiesSet();
        return factory.getObject();
    }

    private JobLauncher createJobLauncher() throws Exception {
        // SimpleJobLauncher just executes a job task on demand
        SimpleJobLauncher jobLauncher = new SimpleJobLauncher();

        jobLauncher.setJobRepository(jobRepository);
        jobLauncher.afterPropertiesSet(); // ensures that properties on jobRepositories have been set

        return jobLauncher;
    }

    private JobExplorer createJobExplorer() throws Exception {
        JobExplorerFactoryBean jobExplorerFactoryBean = new JobExplorerFactoryBean();
        jobExplorerFactoryBean.setDataSource(this.batchDataSource);
        jobExplorerFactoryBean.afterPropertiesSet();
        return jobExplorerFactoryBean.getObject();
    }
}
