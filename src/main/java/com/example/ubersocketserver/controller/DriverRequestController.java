package com.example.ubersocketserver.controller;

import com.example.ubersocketserver.Producers.KafkaProducerService;
import com.example.ubersocketserver.dto.*;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.handler.annotation.MessageMapping;

import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import java.util.Optional;

@RestController
@RequestMapping("/api/socket")
public class DriverRequestController {

    private final SimpMessagingTemplate simpMessagingTemplate;

    private final RestTemplate restTemplate;

    private final KafkaProducerService kafkaProducerService;

    public DriverRequestController(SimpMessagingTemplate simpMessagingTemplate,  RestTemplate restTemplate, KafkaProducerService kafkaProducerService) {
        this.simpMessagingTemplate = simpMessagingTemplate;
        this.restTemplate = restTemplate;
        this.kafkaProducerService = kafkaProducerService;
    }

    @GetMapping
    public Boolean help() {
        kafkaProducerService.publishMessage("sample-topic", "Hello World");
        return true;
    }

    @PostMapping("/newride")
    public ResponseEntity<Boolean> raiseRideRequest(@RequestBody RideRequestDto requestDto) {
        sendDriversNewRideRequest(requestDto);
        return new ResponseEntity<>(Boolean.TRUE, HttpStatus.OK);
    }

    public void sendDriversNewRideRequest(RideRequestDto requestDto) {
        System.out.println("Executed periodic function");
        //Todo : Ideally the request should go to nearby drivers but for simplicity it should go to everyone
        simpMessagingTemplate.convertAndSend("/topic/rideRequest", requestDto);
    }

    @MessageMapping("/rideResponse/{userId}")
    public synchronized void rideResponseHandler(@DestinationVariable String userId, RideResponseDto rideResponseDto) {
        System.out.println(rideResponseDto.getResponse()+ " " + userId);
        UpdateBookingRequestDto requestDto = UpdateBookingRequestDto.builder()
                .driverId(Optional.of(Long.parseLong(userId)))
                .status("SCHEDULED")
                .build();

        try {
            ResponseEntity<UpdateBookingResponseDto> result = this.restTemplate.postForEntity("http://localhost:8000/api/v1/booking/" + rideResponseDto.bookingId,  requestDto, UpdateBookingResponseDto.class);
            simpMessagingTemplate.convertAndSend("/topic/rideResponse/" + userId, "ACCEPTED");
            kafkaProducerService.publishMessage("sample-topic", "Ride assigned to Driver " + userId);
        } catch (Exception e) {
            System.out.println("Booking failed for driver " + userId + ". Error: " + e.getMessage());
            simpMessagingTemplate.convertAndSend("/topic/rideResponse/" + userId, "ALREADY_TAKEN");
        }

        kafkaProducerService.publishMessage("sample-topic", "Hello World");
    }
}
