package com.udmey.vedio.course.bankBalance;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SerdesTest<T> implements  Serde<T> {

	private final ObjectMapper mapper = new ObjectMapper();
	private final Class<T> cls;

	public SerdesTest(Class<T> cls) {
		this.cls = cls;
	}

	@Override
	public void configure(Map configs, boolean isKey) {
	}
 
	@Override
	public void close() {
	}

	public Serializer<T> serializer() {
		return new Serializer<T>() {

			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {

			}

			@Override
			public byte[] serialize(String topic, T data) {
				try {
					return mapper.writeValueAsBytes(data);
				} catch (Exception e) {
					throw new SerializationException("Error serializing JSON message", e);
				}
			}

			@Override
			public void close() {

			}
		};

	}

	@Override
	public Deserializer<T> deserializer() {
		return new Deserializer<T>() {
			@Override
			public void configure(Map<String, ?> configs, boolean isKey) {

			}

			@Override
			public T deserialize(String topic, byte[] data) {
				T result;
				try {
					result = mapper.readValue(data, cls);
				} catch (Exception e) {
					throw new SerializationException(e);
				}

				return result;
			}

			@Override
			public void close() {

			}
		};
	}
}
