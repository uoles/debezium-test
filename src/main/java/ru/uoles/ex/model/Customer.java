package ru.uoles.ex.model;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 20.07.2024
 * Time: 15:19
 */
@Data
@Entity
public class Customer {

    @Id
    private Long id;
    private String fullname;
    private String email;
}