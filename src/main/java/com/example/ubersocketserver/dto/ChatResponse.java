package com.example.ubersocketserver.dto;

import lombok.*;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ChatResponse {
    private String message;
    private String name;
    private String timeStamp;
}
