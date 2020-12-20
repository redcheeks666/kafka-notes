package com.zll.kafka.entities;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class Company {

    private String id;
    private String name;
    private String address;

}
