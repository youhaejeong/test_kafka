package com.yhj.test_kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.time.Duration
import java.util.Properties
import kotlin.concurrent.thread

fun main() {
    // Kafka 브로커 설정
    val bootstrapServers = "localhost:9092"
    val topic = "test-topic"

    // ----------------------------
    // Consumer 스레드 실행
    // ----------------------------
    thread(start = true) {
        val consumerProps = Properties().apply {
            put("bootstrap.servers", bootstrapServers)
            put("group.id", "test-group")
            put("key.deserializer", StringDeserializer::class.java.name)
            put("value.deserializer", StringDeserializer::class.java.name)
            put("auto.offset.reset", "earliest")
        }

        KafkaConsumer<String, String>(consumerProps).use { consumer ->
            consumer.subscribe(listOf(topic))
            println("Consumer started, waiting for messages...")
            while (true) {
                val records = consumer.poll(Duration.ofMillis(100))
                for (record in records) {
                    println("Received: ${record.value()} (key=${record.key()})")
                }
            }
        }
    }

    // ----------------------------
    // Producer 실행 (메시지 전송)
    // ----------------------------
    val producerProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
        put("key.serializer", StringSerializer::class.java.name)
        put("value.serializer", StringSerializer::class.java.name)
    }

    KafkaProducer<String, String>(producerProps).use { producer ->
        for (i in 1..10) {
            val record = ProducerRecord(topic, "key-$i", "Hello Kafka $i")
            producer.send(record)
            println("Sent: ${record.value()}")
            Thread.sleep(200) // 메시지 전송 간격
        }
    }


    println("Producer finished sending messages.")
}
