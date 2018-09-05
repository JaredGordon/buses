package io.jg.buses;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
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

import java.io.IOException;
import java.io.InputStream;

@Configuration
@Slf4j
public class Config {

    @Value("${BUS_API_ENDPOINT}")
    private String busApiEndpoint;

    @Value("${GOOGLE_PUB_CREDENTIALS}")
    private String googlePubCredentials;

    @Value("${GOOGLE_PROJECT_ID}")
    private String googleProjectId;

    @Value("${TOPIC_NAME}")
    private String topicName;

    @Value("${APP_TOKEN}")
    private String appToken;

    @Value("${MAP_TOKEN}")
    private String mapToken;

    @Bean
    public String appToken() {
        return appToken;
    }

    @Bean
    public String mapToken() {
        return mapToken;
    }

    @Bean
    public BusRepository busRepository() {
        return Feign.builder()
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .logger(new Logger.JavaLogger())
                .logLevel(Logger.Level.FULL)
                .target(BusRepository.class, busApiEndpoint);
    }

    @Bean
    public CredentialsProvider credentialsProvider() {
        try {
            InputStream inputStream = Config.class.getResourceAsStream(googlePubCredentials);
            ServiceAccountCredentials serviceAccountCredentials = ServiceAccountCredentials.fromStream(inputStream);
            return FixedCredentialsProvider.create(serviceAccountCredentials);
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

    @Bean
    public Datastore datastore() throws Exception {
        return DatastoreOptions.newBuilder().setCredentials(credentialsProvider().getCredentials()).build().getService();
    }
}
