package com.pluralsight.springbatch.patientbatchloader.config;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import com.pluralsight.springbatch.patientbatchloader.PatientBatchLoaderApp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = PatientBatchLoaderApp.class)
@ActiveProfiles("dev")
class BatchJobConfigurationTest {

    @Autowired
    private Job job;

    @Test
    public void testJobIsConfigured() {
        assertNotNull(job);
        assertEquals(Constants.JOB_NAME, job.getName());
    }

}
