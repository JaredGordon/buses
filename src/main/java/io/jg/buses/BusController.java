package io.jg.buses;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
@Slf4j
public class BusController {

    private BusRepository busRepository;
    private Publisher publisher;
    private String appToken;

    public BusController(BusRepository busRepository, Publisher publisher, String appToken) {
        this.busRepository = busRepository;
        this.publisher = publisher;
        this.appToken = appToken;
    }

    void publishEvents() {
        List<Map<String, Object>> buses = busRepository.getBuses(appToken);
        List<ApiFuture<String>> futures = new ArrayList<>();
        ObjectMapper om = new ObjectMapper();

        String json = null;
        try {
            for (Map<String, Object> event : buses) {
                json = om.writeValueAsString(event);
                log.info("publishing event for segment: " + event.get("segmentid"));
                futures.add(publisher.publish(PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(json)).build()));
            }
        } catch (JsonProcessingException e) {
            log.error("invalid json: " + json, e);
        } finally {
            try {
                List<String> messageIds = ApiFutures.allAsList(futures).get();
                for (String messageId : messageIds) {
                    log.info("published event with id: " + messageId);
                }
            } catch (Exception e) {
                log.error("error retrieving message ids.", e);
            }
        }
    }
}
