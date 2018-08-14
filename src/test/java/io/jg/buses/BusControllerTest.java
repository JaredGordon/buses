package io.jg.buses;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.datastore.*;
import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.util.StreamUtils;

import java.nio.charset.Charset;
import java.util.*;

import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Autowired
    private Datastore datastore;

    @Test
    public void testPublish() {
        busController.publishEvents();
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getBuses(String fileName) throws Exception {
        String contents = StreamUtils.copyToString(new ClassPathResource(fileName).getInputStream(), Charset.defaultCharset());
        return (List<Map<String, Object>>) new ObjectMapper().readValue(contents, List.class);

    }

    @Test
    public void testGetLatest() {
        Set<String[]> buses = busController.getLatest();
        assertNotNull(buses);
    }
}
