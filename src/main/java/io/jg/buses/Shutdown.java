package io.jg.buses;

import com.google.cloud.pubsub.v1.Publisher;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

@Slf4j
@Component
public class Shutdown {

    private Publisher publisher;

    public Shutdown(Publisher publisher) {
        this.publisher = publisher;
    }

    @PreDestroy
    void sayBye() {
        log.info("shutting down the publisher.");
        if (publisher != null) {
            try {
                publisher.shutdown();
            } catch (Exception e) {
                log.error("error shutting down publisher.", e);
            }
        }
    }
}
