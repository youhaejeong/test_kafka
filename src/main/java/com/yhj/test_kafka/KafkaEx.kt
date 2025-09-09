package com.yhj.test_kafka

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.net.HttpURLConnection
import java.net.URL
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.PriorityBlockingQueue
import kotlin.system.exitProcess

fun main() {
    val bootstrapServers = "localhost:9092"
    val topic = "test-topic"
    val partitions = 5            // 파티션 수: key 기반 분배에 사용
    val consumerCount = 3         // Consumer 수: Consumer Group 구성
    val totalMessages = 100       // 테스트용 메시지 수
    val esEndpoint = "http://localhost:9200/kafka-messages/_doc"

    // -------------------------
    // Consumer 그룹 실행
    // -------------------------
    val executor = Executors.newFixedThreadPool(consumerCount)
    repeat(consumerCount) { idx ->
        executor.submit {
            val consumerProps = Properties().apply {
                put("bootstrap.servers", bootstrapServers)
                put("group.id", "test-group")  // 동일 group.id → Consumer Group 구성
                put("key.deserializer", StringDeserializer::class.java.name)
                put("value.deserializer", StringDeserializer::class.java.name)
                put("auto.offset.reset", "earliest")   // 처음부터 읽기
                put("enable.auto.commit", "false")     // 수동 커밋
            }

            KafkaConsumer<String, String>(consumerProps).use { consumer ->
                // -------------------------
                // 파티션 할당/회수 이벤트 처리용 리스너
                // -------------------------
                val listener = object : ConsumerRebalanceListener {
                    // 기존 파티션 회수 시 호출
                    override fun onPartitionsRevoked(partitions: Collection<TopicPartition>) {
                        println("Consumer-$idx partitions revoked: $partitions")
                    }
                    // 새 파티션 할당 시 호출
                    override fun onPartitionsAssigned(partitions: Collection<TopicPartition>) {
                        println("Consumer-$idx partitions assigned: $partitions")
                    }
                }
                // consumer 측 파티션 할당
                consumer.subscribe(listOf(topic), listener)
                println("Consumer-$idx started with rebalance listener")

                // -------------------------
                // 메시지 순서 보장용 큐
                // PriorityBlockingQueue: 멀티스레드 안전, sequence 기준 정렬
                // -------------------------
                val messageQueue = PriorityBlockingQueue<Pair<Long, String>>(11, compareBy { it.first })
                var expectedSeq = 1L

                while (true) {
                    val records = consumer.poll(Duration.ofMillis(500))
                    for (record in records) {
                        // 메시지 value는 "seq:Message 내용" 형식
                        val parts = record.value().split(":", limit = 2)
                        if (parts.size == 2) {
                            val seq = parts[0].toLongOrNull()
                            val message = parts[1]
                            if (seq != null) messageQueue.add(seq to message)
                        }

                        // sequence 순서대로 처리
                        while (messageQueue.isNotEmpty() && messageQueue.peek().first == expectedSeq) {
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

                            expectedSeq++
                        }
                    }

                    // 현재 Consumer가 담당 중인 파티션 확인
//                    val assignedPartitions = consumer.assignment()
//                    if (assignedPartitions.isNotEmpty()) {
//                        println("Consumer-$idx currently assigned partitions: $assignedPartitions")
//                    }
                    // 수동 커밋
                    if(records.count() > 0){
                        try {
                            //현재 처리 완료된 메시지 커밋 // 동기 커밋
                            consumer.commitSync()
                            println("Consumer-$idx committed offsets")

                        }catch (e: Exception){
                            println("Consumer-$idx commit failed: ${e.message}")
                        }
                    }
                }
            }
        }
    }

    // -------------------------
    // Producer 실행
    // -------------------------
    val producerProps = Properties().apply {
        put("bootstrap.servers", bootstrapServers)
        put("key.serializer", StringSerializer::class.java.name)
        put("value.serializer", StringSerializer::class.java.name)
        put("acks", "all")  // 안정성 보장: ISR에 저장 완료 확인
    }

    KafkaProducer<String, String>(producerProps).use { producer ->
        for (i in 1..totalMessages) {
            // key 기반 파티션 분배 → 같은 key는 항상 같은 파티션
            val key = "key-${i % partitions}"
            val record = ProducerRecord(topic, key, "$i:Message 안녕 테스트 카프카 $i")
            try {
                producer.send(record).get()  // 동기 전송
                if (i % 10 == 0) println("Sent $i messages")
            } catch (e: Exception) {
                println("Send failed for seq=$i: ${e.message}")
                exitProcess(1)
            }
        }
        producer.flush() // 버퍼 비우기
    }

    println("Producer finished sending $totalMessages messages")
}

// -------------------------
// Elasticsearch 단건 전송 함수
// -------------------------
fun sendToElasticsearch(endpoint: String, json: String) {
    val url = URL(endpoint)
    val conn = url.openConnection() as HttpURLConnection
    conn.requestMethod = "POST"
    conn.setRequestProperty("Content-Type", "application/json")
    conn.doOutput = true
    conn.outputStream.use { os -> os.write(json.toByteArray()) }

    val responseCode = conn.responseCode
    if (responseCode !in 200..299) {
        throw RuntimeException("Failed to send to ES, response code: $responseCode")
    }
}
