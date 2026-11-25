package com.pedidos360.pedidos.catalog.repository;

import com.pedidos360.pedidos.catalog.entity.Product;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProductRepository extends JpaRepository<Product, Long> {
}