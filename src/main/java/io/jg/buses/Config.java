package io.jg.buses;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.pubsub.v1.ProjectTopicName;
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
import java.io.IOException;

@Configuration
@Slf4j
public class Config {

    @Value("${BUS_API_ENDPOINT}")
    private String busApiEndpoint;

    @Value("${GOOGLE_APPLICATION_CREDENTIALS}")
    private String googleApplicationCredentials;

    @Value("${GOOGLE_PUB_CREDENTIALS}")
    private String googlePubCredentials;

    @Value("${GOOGLE_PROJECT_ID}")
    private String googleProjectId;

    @Value("${TOPIC_NAME}")
    private String topicName;

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
    public GoogleCredentials dataCredentials() {
        File credentialsPath = new File(googleApplicationCredentials);
        try {
            FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
            return ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (Exception e) {
            log.error("unable to load data credentials.", e);
            return null;
        }
    }

    @Bean
    public GoogleCredentials pubCredentials() {
        File credentialsPath = new File(googlePubCredentials);
        try {
            FileInputStream serviceAccountStream = new FileInputStream(credentialsPath);
            return ServiceAccountCredentials.fromStream(serviceAccountStream);
        } catch (Exception e) {
            log.error("unable to load pub credentials.", e);
            return null;
        }
    }

    @Bean
    public BigQuery bigQuery(GoogleCredentials dataCredentials) {
        return BigQueryOptions.newBuilder()
                .setProjectId(googleProjectId)
                .setCredentials(dataCredentials)
                .build()
                .getService();
    }

    @Bean
    public CredentialsProvider pubCredentialsProvider() {
        try {
            return FixedCredentialsProvider.create(ServiceAccountCredentials.fromStream(new FileInputStream(googlePubCredentials)));
        } catch (Exception e) {
            log.error("unable to load pub credentials.", e);
            return null;
        }
    }

    @Bean
    public Publisher publisher(CredentialsProvider pubCredentials) {
        try {
            return Publisher.newBuilder(ProjectTopicName.of(googleProjectId, topicName)).setCredentialsProvider(pubCredentials).build();
        } catch (IOException e) {
            log.error("error creating publisher.", e);
            return null;
        }
    }
}
