package com.example.demo.serialization;

import com.example.demo.entities.Company;
import com.google.gson.Gson;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;
/**
 * 业务中使用序列化工具,避免耦合度高
 * Avro、JSON、Thrift、ProtoBuf或Protostuff等通用的序列化工具来包装
 * */
public class CompanySerializer implements Serializer<Company> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, Company data) {
        System.out.println("~~~~自定义序列化开始~~~~");
        if (data==null) return null;
        byte[] id,name,address;
        try {
            if (data.getId()!=null){
                id = data.getId().getBytes("UTF-8");
            }else {
                id = new byte[0];
            }
            if (data.getName()!=null){
                name = data.getName().getBytes("UTF-8");
            }else {
                name=new byte[0];
            }
            if (data.getAddress()!=null){
                address=data.getAddress().getBytes("UTF-8");
            }else {
                address=new byte[0];
            }
           /**
            * 定义一个buffer,存id name address长度值
            * 读取buffer中属性的时候,需要定义一个字节数组存储读取的值,但定义这个数组的长度无法获取,只能存属性的时候把属性的长度存储在
            * buffer中
            * @Param 4 + 4 + 4 id长度值 address长度值 name长度值
            * @Param name.length name值的长度    address.length address值的长度     id.length id的值的长度
            * put()执行完之后,buffer的position会指向当前存入元素的下一个位置
            * */
            ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + 4 + name.length + address.length + id.length);
            buffer.putInt(id.length);
            buffer.put(id);
            buffer.putInt(name.length);
            buffer.put(name);
            buffer.putInt(address.length);
            buffer.put(address);
            return buffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        System.out.println("序列化失败~");
        return new byte[0];
    }



    @Override
    public void close() {

    }
}
