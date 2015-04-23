package com.jacobo.rabbitMQ;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.AMQP.Queue.BindOk;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.impl.AMQImpl.Queue.DeclareOk;

public class UserMessageManager {
	
	private static final String USER_INBOXES_EXCHANGE = "user-inboxes";
	//el tipo de mensajes que se van a mandar el mime de json
	private static final String MESSAGE_CONTENT_TYPE = "application/vnd.ccm.pmsg.v1+json";
	//codificacion de los mensajes
	private static final String MESSAGE_ENCODING = "UTF-8";
	
	
	@Inject	RabbitMqManager rabbitMqManager;
	/**
	 * el inject sirve para que a partir de una clase cree sus subsistemas dependientes del uml 
	 * tengo el manager ahora necesito el manager del usuario. Ojo que para el funcione el inject tienen que ser
	 * llamados desde el mismo programa
	 */
	
	public void onApplicationStart(){
		
		/**
		 * aqui hago lo mismo que cuando creo algo un Runnable sobreescribo los metodos 
		 * pero lo tengo que hacer sobre el manager del mq no del usuario. Con la anotacion 
		 * Inject no me hace falta llamar al RabbitMqManager como parametro simplemente lo 
		 * traigo, pero tiene que estar en el mismo programa main sino no sirve de nada.
		 * 
		 * En el direct hay que definir el exchange que en este caso es inbox y el tipo que es 
		 * direct, en el canal hay que definir la persistencia y si son autodelete.
		 * 
		 * Con los inbox lo que hace el productor es publicar en los inbox y el MQ se ocupa de repartir los 
		 * mensajes entre los usuarios.
		 * 
		 * Ojo que este no necesita crear el canal. 
		 * 
		 * tampoco crea la instancia de una interfaz
		 * 
		 */
		rabbitMqManager.call(new ChannelCallable<DeclareOk>(){
			
			@Override
			public String getDescription(){
				return "Declaring direct exchange: " + USER_INBOXES_EXCHANGE;
			}
			
			@Override
			public DeclareOk call(final Channel channel) throws IOException{
				String exchange = USER_INBOXES_EXCHANGE;
				String type = "direct";
				// survive a server restart
				boolean durable = true;
				// keep it even if nobody is using it
				boolean autoDelete = false;
				// no special arguments
				Map<String, Object> arguments = null;
				return (DeclareOk) channel.exchangeDeclare(exchange, type, durable,autoDelete, arguments);
			}
		});
	}
	
	/**
	 * metodo de login de usuarios una vez ingresado en la app se le pasa el ID del usuario como 
	 * argumento para crear su buzon de mensajes. Hago lo mismo sobre escribo los metodos del interfaz 
	 * desde un metodo que si lo va a usar
	 * @param userId
	 */
	
	public void onUserLogin(final long userId){
	    final String queue = getUserInboxQueue(userId);
	    rabbitMqManager.call(new ChannelCallable<BindOk>() {
	    	
	        @Override
	        public String getDescription(){
	            return "Declaring user queue: " + queue + ", binding it to exchange: "
	                   + USER_INBOXES_EXCHANGE;
	        }
	 
	        @Override
	        public BindOk call(final Channel channel) throws IOException{
	            return declareUserMessageQueue(queue, channel);
	        }
	    });
	}
	
	/**
	 * este se ocupa de mandar a cada uno de los inbox identificados por el id del usuario, ojo que de ahi no lo 
	 * recibe el usuario este solo lo encamina a su queue
	 * un mensaje del tipo json
	 * @param userID
	 * @param jSonMessage
	 * @return
	 */
	
	public String sendUserMessage(final long userId, final String jsonMessage){
		
		/**
		 * la polimorfia del call me permite trabajar sobre los objetos y devolver lo que quiera
		 */
		return rabbitMqManager.call(new ChannelCallable<String>(){
			/**
			 * descripccion del mensaje 
			 */
		   @Override
		   public String getDescription(){
		            return "Sending message to user: " + userId;
		   }

		   @Override
		   public String call(final Channel channel) throws IOException{
		            String queue = getUserInboxQueue(userId);
		            // it may not exist so declare it
		            declareUserMessageQueue(queue, channel);
		            //le doy un id aleatorio al mensaje
		            String messageId = UUID.randomUUID().toString();
		            /**
		             * configuracion de mensaje en tipo de contenido, codificacion y el ID
		             * para configurarlo no vale con un constructor es un singleton tienes que darle las 
		             * propiedades cuando lo creas con metodos.
		             * El deliveryMode en 2 significa persistente
		             */
		            BasicProperties props = new BasicProperties.Builder()
		                .contentType(MESSAGE_CONTENT_TYPE)
		                .contentEncoding(MESSAGE_ENCODING)
		                .messageId(messageId)
		                .deliveryMode(2)
		                .build();
		            //cola a la que se va a enviar formada con el id del usuario 
		            String routingKey = queue;
		            /**
		             *  publish the message to the direct exchange
		             *  lo pubicas sobre un canal y dentro del canal sobre una cola tambien le tienes que dar 
		             *  las propiedades sobre la publicacion y el mensaje como un flujo de bytes.
		             *  el mensaje puede venir como cualquier cosa, lo que tienes que hacer darle las propiedades
		             *  a la hora de enviar. 
		             */
		            channel.basicPublish(USER_INBOXES_EXCHANGE, routingKey, props,
		                jsonMessage.getBytes(MESSAGE_ENCODING));
		            return messageId;
		        }
		    });
		
	}
	
	/**
	 * este es el que le da los mensajes al usuario como una lista
	 * @param userId
	 * @return
	 */
	
	public List<String> fecthUserMessages(final long userId){
		return rabbitMqManager.call(new ChannelCallable<List<String>>(){
		@Override
		public String getDescription(){
			return " Fetching messages for user: " + userId;
		}
		@Override
		public List<String> call(final Channel channel) throws IOException{
			
			List<String> messages = new ArrayList<>();

			String queue = getUserInboxQueue(userId);
			boolean autoAck = true;
			//objeto de respuesta de la cola
			GetResponse getResponse;
			/**
			 * bucle para obtener todos los mensajes de la cola los obtiene como un objeto GetResponse
			 * una vez obtenido hay que convertirlo en String a traves de las propiedades del mensaje
			 */
			while ((getResponse = channel.basicGet(queue, autoAck)) != null){
				final String contentEncoding = getResponse.getProps().getContentEncoding();
				messages.add(new String(getResponse.getBody(), contentEncoding));
			}

			return messages;
			}
		});
	}
	
	
	/**
	 * dar el nombre string de la cola de mensajes
	 * @param userId que se pasa como argumento
	 * @return nombre del canal
	 */
	
	private String getUserInboxQueue(long userId) {
	        return "user-inbox." + userId;
	}

	/**
	 * metodo privado para crear la cola del usuario.
	 * 
	 * Hay que declarar tanto en el canal como en la cola si los mensajes son persistentes
	 * si se guardan o se eliminan o si son exclusivos. El routing es el nombre de la cola 
	 * 
	 * @param queue nombre de la cola
	 * @param channel canal 
	 * @return un objeto BindOK que quiere decir que eta vinculado
	 * @throws IOException
	 */
	
	private BindOk declareUserMessageQueue(final String queue, final Channel channel) throws IOException{
	    // survive a server restart
	    boolean durable = true;
	    // keep the queue
	    boolean autoDelete = false;
	    // can be consumed by another connection
	    boolean exclusive = false;
	    // no special arguments
	    Map<String, Object> arguments = null;
	    channel.queueDeclare(queue, durable, exclusive, autoDelete, arguments);
	    // bind the addressee's queue to the direct exchange
	    String routingKey = queue;
	    return channel.queueBind(queue, USER_INBOXES_EXCHANGE, routingKey);
	}


}
