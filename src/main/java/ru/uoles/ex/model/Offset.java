package ru.uoles.ex.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 12.10.2024
 * Time: 22:42
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Offset {

    private String id;
    private String key;
    private String value;
    private java.sql.Timestamp timestamp;
    private int pos;
}
