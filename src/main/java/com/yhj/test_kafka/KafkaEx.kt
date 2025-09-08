package com.yhj.test_kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import kotlin.system.exitProcess

fun main() {
    val bootstrapServers = "localhost:9092"
    val topic = "test-topic"
    val partitions = 50           // 파티션 수: 데이터 병렬 처리 단위
    val consumerCount = 10        // Consumer 스레드 수: Consumer Group 내 병렬 처리
    val totalMessages = 1000      // 전송할 메시지 수 (테스트용)
    val esEndpoint = "http://localhost:9200/kafka-messages/_doc" // Elasticsearch 엔드포인트

    // --------------------------------------------------
    // Consumer 그룹 실행 (멀티 스레드로 실행)
    // --------------------------------------------------
    val executor = Executors.newFixedThreadPool(consumerCount)
    repeat(consumerCount) { idx ->
        executor.submit {
            val consumerProps = Properties().apply {
                put("bootstrap.servers", bootstrapServers)
                put("group.id", "test-group")  // 동일 group.id → Consumer Group 구성
                put("key.deserializer", StringDeserializer::class.java.name)
                put("value.deserializer", StringDeserializer::class.java.name)
                put("auto.offset.reset", "earliest") // 토픽 처음부터 읽기
            }

            KafkaConsumer<String, String>(consumerProps).use { consumer ->
                consumer.subscribe(listOf(topic))
                println("Consumer-$idx started")

                // PriorityBlockingQueue: 멀티 스레드 환경에서도 안전하게 정렬
                // key: sequence 번호, value: 메시지 본문
                val messageQueue = PriorityBlockingQueue<Pair<Long, String>>(11, compareBy { it.first })

                while (true) {
                    // poll()로 브로커에서 메시지 가져오기
                    val records = consumer.poll(Duration.ofMillis(100))
                    for (record in records) {
                        // 메시지 예: "1:Message 1" → seq=1, message="Message 1"
                        val parts = record.value().split(":", limit = 2)
                        if (parts.size == 2) {
                            val seq = parts[0].toLongOrNull()
                            val message = parts[1]
                            if (seq != null) messageQueue.add(seq to message)
                        }

                        // 순서 보장 처리: 큐의 head가 다음 처리할 순번일 때만 꺼내기
                        while (messageQueue.isNotEmpty() && messageQueue.peek().first == messageQueue.size.toLong()) {
                            val next = messageQueue.poll()
                            val logMsg = "Consumer-$idx processed seq=${next.first}: ${next.second}"
                            println(logMsg)

                            // -------------------------
                            // Elasticsearch 전송
                            // -------------------------
                            val json = """{"seq": ${next.first}, "message": "${next.second}"}"""

                            try {
                                sendToElasticsearch(esEndpoint, json)
                                println("Consumer-$idx sent to ES: seq=${next.first}, message=${next.second}")
                            } catch (e: Exception) {
                                println("Failed to send to Elasticsearch: ${e.message}")
                            }
                        }
                    }
                }
            }
        }
    }

    // --------------------------------------------------
    // Producer 실행
    // --------------------------------------------------
    val producerProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
        put("key.serializer", StringSerializer::class.java.name)   // key 기반 파티션 분배
        put("value.serializer", StringSerializer::class.java.name)
        put("acks", "all") // 모든 ISR에 저장될 때까지 ack (데이터 안정성 보장)
    }

    KafkaProducer<String, String>(producerProps).use { producer ->
        for (i in 1..totalMessages) {
            // 같은 key는 항상 같은 파티션으로 배정 → 파티션 내 순서 보장
            val key = "key-${i % partitions}"
            val record = ProducerRecord(topic, key, "$i:Message 안녕 나는 카프카 $i")
            try {
                producer.send(record).get() // 동기 전송 → 브로커 저장 확인 후 반환
                if (i % 100 == 0) println("Sent $i messages")
            } catch (e: Exception) {
                println("Send failed for seq=$i: ${e.message}")
                exitProcess(1)
            }
        }
        producer.flush() // 내부 버퍼 비우기
    }

    println("Producer finished sending $totalMessages messages")
}

/**
 * Elasticsearch로 JSON 데이터 전송
 * - HTTP POST 요청
 * - 실패 시 예외 발생
 */
fun sendToElasticsearch(endpoint: String, json: String) {
    val url = URL(endpoint)
    val conn = url.openConnection() as HttpURLConnection
    conn.requestMethod = "POST"
    conn.setRequestProperty("Content-Type", "application/json")
    conn.doOutput = true

    conn.outputStream.use { os ->
        val input = json.toByteArray(StandardCharsets.UTF_8)
        os.write(input, 0, input.size)
    }

    val responseCode = conn.responseCode
    if (responseCode !in 200..299) {
        throw RuntimeException("Elasticsearch returned status $responseCode")
    }
}
