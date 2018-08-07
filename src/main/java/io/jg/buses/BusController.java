package io.jg.buses;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

@RestController
@Slf4j
@Data
public class BusController {

    private BusRepository busRepository;

    public BusController(BusRepository slackRepository) {
        setBusRepository(slackRepository);
    }

    @GetMapping("/buses")
    public ResponseEntity<Set<Map<String, Object>>> getBuses() {
        return new ResponseEntity<>(busRepository.getBuses(), HttpStatus.OK);
    }
}
