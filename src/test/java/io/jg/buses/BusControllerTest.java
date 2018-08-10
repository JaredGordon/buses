package io.jg.buses;

import lombok.extern.slf4j.Slf4j;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
@Slf4j
@Ignore
public class BusControllerTest {

    @Autowired
    private BusController busController;

    @Test
    public void testLoad() {
        long before = busController.segmentCount();
        busController.batchLoad();
        long after = busController.segmentCount();

        assertTrue(before <= after);
    }

    @Test
    public void testPublish() {
        busController.publishEvents();
    }

    @Test
    public void testCount() throws Exception {
        assertTrue(busController.segmentCount() > 0l);
    }
}
