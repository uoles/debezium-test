package ru.uoles.ex.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.uoles.ex.model.Customer;

/**
 * debezium-test
 * Created by Intellij IDEA.
 * Developer: uoles (Kulikov Maksim)
 * Date: 20.07.2024
 * Time: 15:19
 */
@Repository
public interface CustomerRepository extends JpaRepository<Customer, Long> {
}
