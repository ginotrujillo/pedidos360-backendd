package com.pedidos360.pedidos.catalog.service;

import com.pedidos360.pedidos.catalog.dto.*;
import com.pedidos360.pedidos.catalog.entity.Product;
import com.pedidos360.pedidos.catalog.repository.ProductRepository;
import com.pedidos360.pedidos.common.exception.ResourceNotFoundException;
import com.pedidos360.pedidos.messaging.KafkaEventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository repository;
    private final KafkaEventProducer kafkaProducer;

    @Transactional(readOnly = true)
    public List<ProductResponse> list() {
        return repository.findAll().stream().map(ProductResponse::from).toList();
    }

    @Transactional
    public ProductResponse create(ProductRequest req) {
        Product p = Product.builder()
                .name(req.name())
                .description(req.description())
                .price(req.price())
                .stock(req.stock())
                .build();
        Product saved = repository.save(p);
        kafkaProducer.publicarStock(saved);
        return ProductResponse.from(saved);
    }

    @Transactional
    public ProductResponse update(Long id, ProductRequest req) {
        Product p = repository.findById(id)
                .orElseThrow(() -> new ResourceNotFoundException("Producto " + id + " no encontrado"));
        p.setName(req.name());
        p.setDescription(req.description());
        p.setPrice(req.price());
        p.setStock(req.stock());
        kafkaProducer.publicarStock(p);
        return ProductResponse.from(p);
    }
}