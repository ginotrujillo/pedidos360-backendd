package com.pedidos360.pedidos.orders.dto;

import com.pedidos360.pedidos.orders.entity.OrderStatus;
import jakarta.validation.constraints.NotNull;

public record UpdateStatusRequest(@NotNull OrderStatus status) {}