package com.jacobo.rabbitMQ;

import java.io.IOException;

import com.rabbitmq.client.Channel;

public interface ChannelCallable<T>{
	
	/**
	 * metodo para obtener la descripcion de lo que le pincho 
	 * @return
	 */
	String getDescription();
	
	/**
	 * metodo polimorfico
	 * @param channel
	 * @return objeto del tipo T con el que llama a la interfaz
	 * @throws IOException
	 */
	
	T call(Channel channel) throws IOException;
	
}
