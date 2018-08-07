package io.jg.buses;

import feign.Feign;
import feign.Logger;
import feign.gson.GsonDecoder;
import feign.gson.GsonEncoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class Config {

    @Bean
    public BusRepository slackRepository() {
        return Feign.builder()
                .encoder(new GsonEncoder())
                .decoder(new GsonDecoder())
                .logger(new Logger.JavaLogger())
                .logLevel(Logger.Level.FULL)
                .target(BusRepository.class, "https://data.cityofchicago.org/resource/8v9j-bter.json");
    }
}
