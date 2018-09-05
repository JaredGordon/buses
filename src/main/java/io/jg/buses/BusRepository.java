package io.jg.buses;

import feign.Headers;
import feign.Param;
import feign.RequestLine;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public interface BusRepository {

    @RequestLine("GET /")
    @Headers({"X-App-Token: {token}"})
    public List<Map<String, Object>> getBuses(@Param("token") String token);
}
