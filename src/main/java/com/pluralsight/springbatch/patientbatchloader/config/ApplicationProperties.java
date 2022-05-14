package com.pluralsight.springbatch.patientbatchloader.config;

import java.io.File;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Properties specific to Patient Batch Loader.
 * <p>
 * Properties are configured in the application.yml file.
 */
@ConfigurationProperties(prefix = "application", ignoreUnknownFields = false)
public class ApplicationProperties {

    private final Batch batch = new Batch();

    public Batch getBatch() {
        return batch;
    }

    public static class Batch {
        private String inputPath = new File("data").getAbsolutePath();

        public String getInputPath() {
            return this.inputPath;
        }

        public void setInputPath(String inputPath) {
            this.inputPath = inputPath;
        }
    }
}
