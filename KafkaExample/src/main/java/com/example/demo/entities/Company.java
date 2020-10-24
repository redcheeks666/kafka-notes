package com.example.demo.entities;

import lombok.Data;

@Data
public class Company {
    private String id;
    private String name;
    private String address;

    public Company(String id, String name, String address) {
        this.id = id;
        this.name = name;
        this.address = address;
    }

    public Company() {
    }
}
