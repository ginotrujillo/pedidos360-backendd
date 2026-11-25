package com.pedidos360.pedidos.messaging;

import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * pedidos-service es PRODUCTOR: solo declara los exchanges.
 * Las colas, DLQ y bindings los declara eventos-service (que es quien las consume).
 */
@Configuration
public class RabbitMQConfig {

    @Value("${app.rabbitmq.exchange.direct}")
    private String directExchangeName;
    @Value("${app.rabbitmq.exchange.topic}")
    private String topicExchangeName;

    @Bean
    public MessageConverter jsonMessageConverter() {
        return new JacksonJsonMessageConverter();
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
        RabbitAdmin rabbitAdmin = new RabbitAdmin(connectionFactory);
        rabbitAdmin.setAutoStartup(true);
        return rabbitAdmin;
    }

    @Bean
    public ApplicationRunner initializeRabbit(RabbitAdmin rabbitAdmin) {
        return args -> rabbitAdmin.initialize();
    }

    @Bean
    DirectExchange directExchange() {
        return ExchangeBuilder.directExchange(directExchangeName).durable(true).build();
    }

    @Bean
    TopicExchange topicExchange() {
        return ExchangeBuilder.topicExchange(topicExchangeName).durable(true).build();
    }
}
