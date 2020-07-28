package com.example.demo.serialization;

import com.example.demo.entities.Company;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.util.Map;
/**
 * 业务中使用序列化工具,避免耦合度高
 * Avro、JSON、Thrift、ProtoBuf或Protostuff等通用的序列化工具来包装
 * */
public class CompanyDeserializer implements Deserializer<Company> {
    /**
     * 配置当前类
     * */
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }
    /**
     * 反序列化逻辑
     * */
    @Override
    public Company deserialize(String topic, byte[] data) {
        System.out.println("~~~~自定义反序列化开始~~~~");
        if (data==null) return null;
        String id,name,address;
        Company company = new Company();
        try {
            if (data.length<12) throw new SerializationException("data size Exception");
            //wrap可以把字节数组包装成缓冲区ByteBuffer
            ByteBuffer buffer = ByteBuffer.wrap(data);
            //按顺序取长度//按顺序取值
            int idLength = buffer.getInt();
            byte[] idBytes = new byte[idLength];
            buffer.get(idBytes);
            int nameLength = buffer.getInt();
            byte[] nameBytes = new byte[nameLength];
            buffer.get(nameBytes);
            int addressLength = buffer.getInt();
            byte[] addressBytes = new byte[addressLength];
            buffer.get(addressBytes);
            id = new String(idBytes, "UTF-8");
            name = new String(nameBytes, "UTF-8");
            address = new String(addressBytes, "UTF-8");
            company = new Company(id,name,address);
            return company;
        }catch (Exception e){
            e.printStackTrace();
        }
        System.out.println("反序列化失败~");
        return null;
    }

    /**
     * 关闭当前序列化器
     * */
    @Override
    public void close() {

    }
}
