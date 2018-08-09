package io.jg.buses;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import feign.Feign;
import feign.Logger;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.File;
import java.io.FileInputStream;

@Configuration
@Slf4j
public class Config {

    @Value("${BUS_API_ENDPOINT}")
    private String busApiEndpoint;

    @Value("${GOOGLE_APPLICATION_CREDENTIALS}")
    private String googleApplicationCredentials;

    @Value("${GOOGLE_PROJECT_ID}")
    private String googleProjectId;

    @Value("${APP_TOKEN}")
    public String appToken;

    @Bean
    public String appToken() {
        return appToken;
    }

    @Bean
    public BusRepository slackRepository() {
        return Feign.builder()
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .logger(new Logger.JavaLogger())
                .logLevel(Logger.Level.FULL)
                .target(BusRepository.class, busApiEndpoint);
    }

    @Bean
    public GoogleCredentials credentials() {
        File credentialsPath = new File(googleApplicationCredentials);
        try {
            FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
            return ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (Exception e) {
            log.error("unable to load credentials.", e);
            return null;
        }
    }

    @Bean
    public BigQuery bigQuery(GoogleCredentials googleCredentials) {
        return BigQueryOptions.newBuilder()
                .setProjectId(googleProjectId)
                .setCredentials(googleCredentials)
                .build()
                .getService();
    }
}
