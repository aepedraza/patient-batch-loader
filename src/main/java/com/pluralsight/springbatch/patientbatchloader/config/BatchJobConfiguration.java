package com.pluralsight.springbatch.patientbatchloader.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.JobParametersValidator;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.util.StringUtils;

@Configuration
public class BatchJobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private ApplicationProperties applicationProperties;

    @Bean
    JobRegistryBeanPostProcessor jobRegistryBeanPostProcessor(JobRegistry jobRegistry) {
        JobRegistryBeanPostProcessor postProcessor = new JobRegistryBeanPostProcessor();
        postProcessor.setJobRegistry(jobRegistry);
        return postProcessor;
    }

    @Bean
    public Job job(Step step) {
        return this.jobBuilderFactory
            .get(Constants.JOB_NAME)
            .validator(validator())
            .start(step)
            .build();
    }

    @Bean
    public JobParametersValidator validator() {
        return parameters -> {
            String fileName = parameters.getString(Constants.JOB_PARAM_FILE_NAME);
            validateFileName(fileName);

            String pathFileName = applicationProperties.getBatch().getInputPath() + File.separator + fileName;
            validatePath(pathFileName);
        };
    }

    private void validateFileName(String fileName) throws JobParametersInvalidException {
        if (StringUtils.isBlank(fileName)) {
            throw new JobParametersInvalidException(
                "The patient-batch-loader.fileName parameter is required.");
        }
    }

    private void validatePath(String fullName) throws JobParametersInvalidException {
        try {
            Path file = Paths.get(fullName);
            if (Files.notExists(file) || !Files.isReadable(file)) {
                throw new Exception("File did not exist or was not readable");
            }
        } catch (Exception e) {
            throw new JobParametersInvalidException(String.format("%s is not a valid file location", fullName));
        }
    }

    @Bean
    public Step step() {
        return this.stepBuilderFactory
            .get(Constants.STEP_NAME)
            // tasklet: Defines what task the step will perform
            .tasklet((contribution, chunkContext) -> {
                // StepContribution contribution: Buffer to store details of processing (read, count, etc)
                // ChunkContext chunkContext: context object for data stored during the duration of a chunk
                System.err.println("Hello World!");
                return RepeatStatus.FINISHED; // tell Spring Batch that processing is complete
            })
            .build();
    }
}
