package io.jg.buses;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@Profile({"!test"})
public class BusPull {

    @Autowired
    private BusController busController;

    @Scheduled(fixedRateString = "${BUS_PULL_INTERVAL}")
    public void pull() {
        log.info("loading more segments ...");
        busController.publishEvents();
        log.info("... done!");
    }
}
