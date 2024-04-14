package shared.messages;

import java.io.*;
import java.net.Socket;

import org.apache.log4j.Logger;

import shared.messages.SimpleKVMessage;
import shared.messages.KVMessage.StatusType;

import ecs.IECSNode;

public final class SimpleKVCommunication {

    private static final int BUFFER_SIZE = 1024;
    private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	private static final String SECRET_TOKEN = "secret";

    public static String receiveMessage(InputStream input, Logger logger) throws IOException {
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];
		
		/* read first char from stream */
		byte read = (byte) input.read();	
		boolean reading = true;
		
		while(read != 13 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if(index == BUFFER_SIZE) {
				if(msgBytes == null){
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			} 
			
			/* only read valid characters, i.e. letters and constants */
			if((read > 31 && read < 127)) {
				bufferBytes[index] = read;
				index++;
			}
			
			/* stop reading is DROP_SIZE is reached */
			if(msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
			
			/* read next char from stream */
			read = (byte) input.read();
		}
		
		if(msgBytes == null){
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}
		
		msgBytes = tmp;
		
		/* build final String */
		String msg = new String(msgBytes);
		return msg;
    }

    public static SimpleKVMessage parseMessage(String msg, Logger logger) {
		logger.info("Received request string: " + msg);
        if (msg == null || msg.trim().isEmpty()) {
            logger.error("Empty or null request string received");
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }
        String[] parts = msg.split(" ", 3);
        StatusType status;
        try {
            status = StatusType.valueOf(parts[0]);
            logger.info("Parsed status: " + status);
        } catch (IllegalArgumentException e) {
            logger.error("Invalid Status:" + parts[0]);
            return new SimpleKVMessage(StatusType.PUT_ERROR, null, null);
        }

		SimpleKVMessage ret_msg;
        if (status == StatusType.SERVER_NOT_RESPONSIBLE || status == StatusType.SERVER_STOPPED || 
       		status == StatusType.SERVER_WRITE_LOCK || status == StatusType.KEYRANGE || 
			status == StatusType.KEYRANGE_SUCCESS || status == StatusType.KEYRANGE_READ || 
			status == StatusType.KEYRANGE_READ_SUCCESS) {
				String parsed_msg = parts.length > 1 ? parts[1] : null;
				ret_msg = new SimpleKVMessage(status, parsed_msg);
				logger.info("Extracted message: " + parsed_msg);
				// System.out.println("HELLO I AM HERERERERERERER");
		} else {
			String key = parts.length > 1 ? parts[1] : null;
        	String value = parts.length > 2 ? parts[2] : null;
			ret_msg = new SimpleKVMessage(status, key, value);
			logger.info("Extracted key: " + key + ", value: " + value);
		}
		System.out.println("SimpleKVComm: " + ret_msg.getMsg());
        return ret_msg;
    }

    public static void sendMessage(SimpleKVMessage msg, OutputStream output, Logger logger) throws IOException {
		System.out.println("SimpleKVComm, SENDING MESSAGE");
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
		logger.info("Send message:\t '" + msg.getMsg() + "'");
    }

	public static void sendToServer(StatusType command, String key, String value, IECSNode node, Logger logger) {
        String new_command = SECRET_TOKEN + " " + command + " false " + key + (value != null ? (" " + value) : "");
        logger.info("Sending command to KVServer: " + new_command);
        
        try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
            out.println(new_command);
            logger.info("SendToServer Called: " + command + ", " + key + ", " + value);
        } catch (IOException e) {
            logger.error("Error sending to node: " + e.getMessage());
        }
    }


	public static void ServerToServer(StatusType command, String key, String value, IECSNode node, boolean isReplication, Logger logger) {
		String actualCommand = SECRET_TOKEN + " " + command + " " + isReplication + " " + key + (value != null ? " " + value : "");
		System.out.println("ServerToServer, actualCommand:" + actualCommand);
	
		// Send the message using the socket connection
		try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
			 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
			out.println(actualCommand);
			logger.info("ServerToServer Called: " + command + ", " + key + ", " + value + ", isReplication: " + isReplication);
		} catch (IOException e) {
			logger.error("Error sending to node: " + e.getMessage());
		}
	}

	
	// public static void ServerToServer(StatusType command, String key, String value, IECSNode node, boolean isReplication, Logger logger) {
	// 	String actualCommand = SECRET_TOKEN + " " + command + " " + key + (value != null ? (" " + value) : "") + " "+ isReplication ;
	// 	System.out.println("ServerToServer:" + actualCommand); 

	// 	logger.info("ServerToServer, Preparing to send command to KVServer: " + actualCommand);

	// 	// Send the message using the socket connection
	// 	try (Socket socket = new Socket(node.getNodeHost(), node.getNodePort());
    //         PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {
    //         out.println(actualCommand);
    //         logger.info("ServerToServer Called: " + command + ", " + key + ", " + value);
    //     } catch (IOException e) {
    //         logger.error("Error sending to node: " + e.getMessage());
    //     }
	// }

	
}