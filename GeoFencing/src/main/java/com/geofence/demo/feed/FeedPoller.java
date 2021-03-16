package com.geofence.demo.feed;

import com.geofence.demo.entity.Location;
import com.geofence.demo.entity.VehiclePositionFeed;
import com.google.transit.realtime.GtfsRealtime;
import kong.unirest.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.io.InputStream;

@Component
@EnableScheduling
public class FeedPoller {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    @Value("${rtd.username}")
    private String rtdUsername;

    @Value("${rtd.password}")
    private String rtdPassword;

    @Autowired
    KafkaTemplate<String, VehiclePositionFeed> kafkaTemplate;

    @Scheduled(cron = "*/30 * * * * *")
    private void getBusPositions(){
        try{

            logger.info("Getting latest vehicle positions from RTD feed.");

            // get latest vehicle positions
            String userName = "RTDgtfsRT";
            String password = "realT!m3Feed";
            String url = "http://www.rtd-denver.com/google_sync/VehiclePosition.pb";

            Unirest.get(url)
                    .basicAuth(userName, password)
                    .thenConsume(rawResponse -> {
                        try {
                            InputStream stream = rawResponse.getContent();

                            GtfsRealtime.FeedMessage feed = GtfsRealtime.FeedMessage.parseFrom(stream);

                            for (GtfsRealtime.FeedEntity entity : feed.getEntityList()) {
                                GtfsRealtime.VehiclePosition vehiclePosition = entity.getVehicle();

                                Location location = new Location();
                                location.setLatitude(vehiclePosition.getPosition().getLatitude());
                                location.setLongitude(vehiclePosition.getPosition().getLongitude());

                                VehiclePositionFeed busPosition = new VehiclePositionFeed();

                                busPosition.setId(vehiclePosition.getVehicle().getId());
                                busPosition.setTimeStamp(vehiclePosition.getTimestamp() * 1000); // convert seconds to Avro-friendly millis
                                busPosition.setLocation(location);
                                busPosition.setBearing(vehiclePosition.getPosition().getBearing());

                                Message<VehiclePositionFeed> message = MessageBuilder
                                        .withPayload(busPosition)
                                        .setHeader(KafkaHeaders.TOPIC, "rtd-bus-position")
                                        .setHeader(KafkaHeaders.MESSAGE_KEY, entity.getVehicle().getVehicle().getId())
                                        .build();

                                // publish to `rtd-bus-position` Kafka topic
                                kafkaTemplate.send(message);
                            }
                        }catch (Exception e){
                        e.printStackTrace();
                        }
                    });

                        }catch (Exception e){
            e.printStackTrace();
        }
    }

}


