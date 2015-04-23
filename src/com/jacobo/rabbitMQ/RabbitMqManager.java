package com.jacobo.rabbitMQ;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

public class RabbitMqManager implements ShutdownListener {
	
	/**
	 * Objeto iniciador de la conexion ConnectionFactory a este objeto se le pasan los argumentos de puerto, ip
	 * usuario y contrasea 
	 * 
	 * executor es el objeto para recuperarse de las exceptions
	 * 
	 * connection es para conectar realmente apartir del factory
	 * 
	 */
	
	private final static Logger LOGGER = Logger.getLogger(RabbitMqManager.class.getName());
	private final ConnectionFactory factory;
	private final ScheduledExecutorService executor;
	private volatile Connection connection;
	
	
	/**
	 * constructor de la clase indica donde se tiene que crear el log asi como la conexion
	 */
	public RabbitMqManager(String ruta, final ConnectionFactory factory){
		initLogger(ruta);
		this.factory = factory;
		executor = Executors.newSingleThreadScheduledExecutor();
		connection = null;
	}
	/**
	 * Metodo para iniciar la conexion de verdad contra el gestor de MQ
	 */
	public void start(){
			try {
				connection = factory.newConnection();
				connection.addShutdownListener(this);
				LOGGER.info("conectado a " + factory.getHost() + " al puerto " + factory.getPort());
			} catch (IOException e) {
				LOGGER.log(Level.SEVERE, "Failed to connect to " +
						factory.getHost() + ":" + factory.getPort(), e);
				asyncWaitAndReconnect();
				e.printStackTrace();
			}
	}
	
	/**
	 * metodo para crear el canal en donde se crearan las cosas de envio de mensajes.
	 * 
	 * el canal de conexion es en si mismo un objeto tiene que crearlo desde la coexion y despues darle 
	 * propiedades pero desde el gestor o desde el productor
	 */
	
	public Channel createChannel(){
		try{
			return connection == null ? null : connection.createChannel();
		}catch (final Exception e){
			LOGGER.log(Level.SEVERE, "Failed to create channel", e);
			return null;
			}
	}
	
	/**
	 * metodo para cerrar la conexion de un canal tanto si esta abierto como si se null
	 */
	public void closeChannel(final Channel channel){
	// isOpen is not fully trustable!
		if ((channel == null) || (!channel.isOpen())){
			return;
		}
		try{
			channel.close();
		}catch (final Exception e){
			LOGGER.log(Level.SEVERE, "Failed to close channel: " + channel,
					e);
		}
	}
	
	/**
	 * el call esta definido en la interfaz pero no lo aado con un implements para no tener que 
	 * sobrescribir los metodo
	 * @param callable le paso como parametro una interfaz polimorfica y creo el canal con el metodo 
	 * createChannel() y devolvera un polimorfico T que en este caso es un channel cuando termina llama al 
	 * closechannel.
	 * al usar una clase polimorfica lo que puedo hacer es redefinir en un new como con el Runnable 
	 * @return  
	 */
	
	public <T> T call(final ChannelCallable<T> callable){
		final Channel channel = createChannel();
		if (channel != null){
			try{
				return callable.call(channel);
			} catch (final Exception e){
				LOGGER.log(Level.SEVERE, "Failed to run: " +
						callable.getDescription() + " on channel: " + channel, e);
			} finally{
				closeChannel(channel);
			}
		}
		return null;
	}
	
	@Override
	public void shutdownCompleted(ShutdownSignalException cause) {	
		// reconnect only on unexpected errors
		if (!cause.isInitiatedByApplication()){
			LOGGER.log(Level.SEVERE, "Lost connection to " +
					factory.getHost() + ":" + factory.getPort(),
					cause);
			connection = null;
			asyncWaitAndReconnect();
		}
	}
	
	/**
	 * Metodo para la reconexion en exception recuperacion hacia delante, con executor puedo pedir un 
	 * que el hilo sea replanificable cn schedule  cada 15 segundos, con execute solo lanza una vez
	 */
	private void asyncWaitAndReconnect() {
		executor.schedule(new Runnable()
		{
		@Override
		public void run(){
			start();
		}
		}, 15, TimeUnit.SECONDS);
	}
	
	/**
	 * con executor igual que puedo planificar puedo parar hilos de una clase. 
	 * Con executor si me falla un hilo puedo volver a ejecutarlo con otro ID
	 * Puedo tenerlo dando vueltas hasta llegar a un punto
	 */
	
	public void stop(){
		executor.shutdownNow();
		if (connection == null){
			return;
		}
		try{
			connection.close();
		}catch (final Exception e){
			LOGGER.log(Level.SEVERE, "Failed to close connection", e);
		}
		finally{
			connection = null;
		}
	}

	/**
	 * metodo para iniciar el logger en la ruta pasada como parametro
	 * @param ruta
	 */
	private void initLogger(String ruta){
		try {
			LOGGER.addHandler(new FileHandler(ruta));
		} catch (SecurityException | IOException e) {
			System.out.println("no se puede inicializar el logger");
			e.printStackTrace();
		};
	}

}
