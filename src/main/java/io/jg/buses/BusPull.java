package io.jg.buses;

import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class BusPull {

    @Scheduled(fixedRate = 5000)
    public void pull() {
        log.info("hello!");
    }
}
