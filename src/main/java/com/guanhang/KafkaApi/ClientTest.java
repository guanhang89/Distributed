package com.guanhang.KafkaApi;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.requests.*;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 客户端API管理
 */
public class ClientTest {

    //返送请求的主方法
    public ByteBuffer send(String host, int port, AbstractRequest request, ApiKeys apiKeys) throws IOException {
        Socket socket = connect(host, port);
        try {
            return send(request, apiKeys, socket);
        }finally {
            socket.close();
        }
    }

    //建立连接
    private Socket connect(String host, int port) throws IOException {
        return new Socket(host, port);
    }


    //向给定的Socket发送请求
    private ByteBuffer send(AbstractRequest request, ApiKeys apiKeys, Socket socket) throws IOException {
        RequestHeader header = new RequestHeader(apiKeys.id, request.version(), "client-id", 0);
        ByteBuffer buffer = ByteBuffer.allocate(header.sizeOf() + request.sizeOf());
        header.writeTo(buffer);
        request.writeTo(buffer);
        byte[] seializedRequest = buffer.array();
        byte[] response = issueRequestAndWaitForResponse(socket, seializedRequest);
        ByteBuffer responseBuffer = ByteBuffer.wrap(response);
        ResponseHeader.parse(responseBuffer);
        return responseBuffer;

    }
    //发送序列化请求并等待response返回
    private byte[] issueRequestAndWaitForResponse(Socket socket, byte[] request) throws IOException {
        sendRequest(socket, request);
        return getResponse(socket);
    }

    private byte[] getResponse(Socket socket) throws IOException {
        DataInputStream dis = null;
        try {
            dis = new DataInputStream(socket.getInputStream());
            byte[] bytes = new byte[dis.readInt()];
            dis.readFully(bytes);
            return bytes;
        } catch (IOException e) {
            if (dis != null) {
                dis.close();
            }
        }
        return null;
    }

    private void sendRequest(Socket socket, byte[] request) throws IOException {
        DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
        dos.writeInt(request.length);
        dos.write(request);
        dos.flush();
    }

    //创建Topic
    public void createTopoics(String topicName, int partitions, short replicationFactor) throws IOException {
        Map<String, CreateTopicsRequest.TopicDetails> topics = new HashMap<>();
        topics.put(topicName, new CreateTopicsRequest.TopicDetails(partitions, replicationFactor));
        int createionTimeoutMs = 60000;
        CreateTopicsRequest request = new CreateTopicsRequest.Builder(topics, createionTimeoutMs).build();
        ByteBuffer response = send("localhost", 9092, request, ApiKeys.CREATE_TOPICS);
        CreateTopicsResponse.parse(response, request.version());
    }

    //删除topic
    public void deleteTopics(Set<String> topics) throws IOException {
        int deleteTimeoutMs = 30000;
        DeleteTopicsRequest request = new DeleteTopicsRequest.Builder(topics, deleteTimeoutMs).build();
        ByteBuffer response = send("localhost", 9092, request, ApiKeys.DELETE_TOPICS);
        DeleteTopicsRequest.parse(response, request.version());
    }

    //获取某个consumer group下所有topic分区的位移信息
    public Map<TopicPartition,OffsetFetchResponse.PartitionData> getAllOffsetForGroup(String groupId) throws IOException {
        OffsetFetchRequest request = new OffsetFetchRequest.Builder(groupId, null).setVersion((short) 2).build();
        ByteBuffer response = send("localhost", 9092, request, ApiKeys.OFFSET_FETCH);
        OffsetFetchResponse resp = OffsetFetchResponse.parse(response, request.version());
        return resp.responseData();
    }

    //查询某个consumer group下的某个topic分区的位移
    public void getOffsetForPartition(String groupID, String topic, int partition) throws IOException {
        TopicPartition tp = new TopicPartition(topic, partition);
        OffsetFetchRequest request = new OffsetFetchRequest.Builder(groupID, Collections.singletonList(tp)).setVersion((short) 2).build();
        ByteBuffer response = send("localhost", 9092, request, ApiKeys.OFFSET_FETCH);
        OffsetFetchResponse resp = OffsetFetchResponse.parse(response, request.version());
        OffsetFetchResponse.PartitionData partitionData = resp.responseData().get(tp);
        System.out.println(partitionData.offset);
    }
}
