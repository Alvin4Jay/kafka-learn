package com.jay.kafka.chapter2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author xuweijie
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Company {
    private String name;
    private String address;
}
