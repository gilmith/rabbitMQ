package com.jacobo.rabbitMQ;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.rabbitmq.client.ConnectionFactory;

public class RabbitMqMain {

	public static void main(String[] args) {
		if(args.length == 0){ 
			System.out.println("Error de parametros hay que pasar la ruta");
		} else {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("/home/jake/git/rabbitMQ/rabbitMQ/src/com/jacobo/rabbitMQ/rabb"));
				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost(properties.getProperty("servidor"));
				factory.setPort(Integer.parseInt(properties.getProperty("puerto")));
				factory.setUsername(properties.getProperty("usuario"));
				factory.setPassword(properties.getProperty("clave"));
				RabbitMqManager manager = new RabbitMqManager(args[0], factory);
				manager.start();
				
				UserMessageManager userMessageManager = new UserMessageManager();
				userMessageManager.onApplicationStart();
				
				for(int i = 0; i <= 10; i++){
					userMessageManager.onUserLogin(i);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

			
		}

	}

}
