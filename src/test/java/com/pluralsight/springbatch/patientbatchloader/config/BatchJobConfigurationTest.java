package com.pluralsight.springbatch.patientbatchloader.config;

import java.util.HashMap;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.MetaDataInstanceFactory;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.batch.test.StepScopeTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import com.pluralsight.springbatch.patientbatchloader.PatientBatchLoaderApp;
import com.pluralsight.springbatch.patientbatchloader.domain.PatientEntity;
import com.pluralsight.springbatch.patientbatchloader.domain.PatientRecord;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(classes = PatientBatchLoaderApp.class)
@ActiveProfiles("dev")
@TestExecutionListeners({
    DependencyInjectionTestExecutionListener.class, // Enables DI and initialization of test instances
    StepScopeTestExecutionListener.class // Sets up step scope context
})
class BatchJobConfigurationTest {

    private JobParameters jobParameters;

    @Autowired
    private Job job;

    @Autowired
    private FlatFileItemReader<PatientRecord> reader;

    @Autowired
    private Function<PatientRecord, PatientEntity> processor;

    @BeforeEach
    public void setupJobParameters() {
        HashMap<String, JobParameter> params = new HashMap<>();
        params.put(Constants.JOB_PARAM_FILE_NAME, new JobParameter("test-unit-testing.csv"));
        jobParameters = new JobParameters(params);
    }

    @Test
    public void testJobIsConfigured() {
        assertNotNull(job);
        assertEquals(Constants.JOB_NAME, job.getName());
    }

    @Test
    public void testReader() {
        StepExecution stepExecution = MetaDataInstanceFactory.createStepExecution(jobParameters);

        try {
            int count = StepScopeTestUtils.doInStepScope(stepExecution,
                () -> countRecordsAndAssertPatients(stepExecution));
            assertThat(count).isEqualTo(1);
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    private int countRecordsAndAssertPatients(StepExecution stepExecution) throws Exception {
        int numPatients = 0;
        PatientRecord patient;

        try {
            reader.open(stepExecution.getExecutionContext());
            while ((patient = reader.read()) != null) {
                assertTestPatient(patient);
                numPatients++;
            }
        } finally {
            closeReader();
        }

        return numPatients;
    }

    private void assertTestPatient(PatientRecord patient) {
        assertNotNull(patient);
        assertEquals("72739d22-3c12-539b-b3c2-13d9d4224d40", patient.getSourceId());
        assertEquals("Hettie", patient.getFirstName());
        assertEquals("P", patient.getMiddleInitial());
        assertEquals("Schmidt", patient.getLastName());
        assertEquals("rodo@uge.li", patient.getEmailAddress());
        assertEquals("(805) 384-3727", patient.getPhoneNumber());
        assertEquals("Hutij Terrace", patient.getStreet());
        assertEquals("Kahgepu", patient.getCity());
        assertEquals("ID", patient.getState());
        assertEquals("40239", patient.getZip());
        assertEquals("6/14/1961", patient.getBirthDate());
        assertEquals("I", patient.getAction());
        assertEquals("071-81-2500", patient.getSsn());
    }

    private void closeReader() {
        try {
            reader.close();
        } catch (Exception e) {
            fail(e.toString());
        }
    }

    @Test
    public void testProcessor() {
        PatientRecord patientRecord = recordForTest();
        PatientEntity entity = processor.apply(patientRecord);
        assertEntity(entity);
    }

    private PatientRecord recordForTest() {
        return new PatientRecord(
            "72739d22-3c12-539b-b3c2-13d9d4224d40",
            "Hettie",
            "P",
            "Schmidt",
            "rodo@uge.li",
            "(805) 384-3727",
            "Hutij Terrace",
            "Kahgepu",
            "ID",
            "40239",
            "6/14/1961",
            "I",
            "071-81-2500");
    }

    private void assertEntity(PatientEntity entity) {
        assertNotNull(entity);
        assertEquals("72739d22-3c12-539b-b3c2-13d9d4224d40", entity.getSourceId());
        assertEquals("Hettie", entity.getFirstName());
        assertEquals("P", entity.getMiddleInitial());
        assertEquals("Schmidt", entity.getLastName());
        assertEquals("rodo@uge.li", entity.getEmailAddress());
        assertEquals("(805) 384-3727", entity.getPhoneNumber());
        assertEquals("Hutij Terrace", entity.getStreet());
        assertEquals("Kahgepu", entity.getCity());
        assertEquals("ID", entity.getState());
        assertEquals("40239", entity.getZipCode());
        assertEquals(14, entity.getBirthDate().getDayOfMonth());
        assertEquals(6, entity.getBirthDate().getMonthValue());
        assertEquals(1961, entity.getBirthDate().getYear());
        assertEquals("071-81-2500", entity.getSocialSecurityNumber());
    }

}
