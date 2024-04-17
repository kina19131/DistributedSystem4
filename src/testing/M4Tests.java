package testing;

import org.junit.Test;
import app_kvServer.KVServer;
import client.KVStore;
import app_kvECS.ECSClient;

import junit.framework.TestCase;
import shared.messages.KVMessage;
import shared.messages.KVMessage.StatusType;
import java.io.File;
import java.io.FilenameFilter;

import org.junit.AfterClass;

public class M4Tests extends TestCase {
    private ECSClient ecsClient;
    private KVStore kvClient;

    @Override
    public void setUp() {
        try {
            kvClient = new KVStore("localhost", 50002);
            kvClient.connect();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void tearDown() {
        if (kvClient != null) {
            kvClient.disconnect();
        }

        // // Specify the directory where the files are located
        // File dir = new File(".");

        // // Filter to identify files that match the pattern kvstorage_*.txt
        // FilenameFilter filter = new FilenameFilter() {
        //     public boolean accept(File dir, String name) {
        //         return (name.startsWith("kvstorage_") || name.startsWith("userCredStorage_")) && name.endsWith(".txt");
        //     }
        // };

        // // List all files that match the filter
        // File[] files = dir.listFiles(filter);

        // // Delete each file that matches the pattern
        // if (files != null) {
        //     for (File file : files) {
        //         if (file.delete()) {
        //             System.out.println("Deleted the file: " + file.getName());
        //         } else {
        //             System.out.println("Failed to delete the file: " + file.getName());
        //         }
        //     }
        // } else {
        //     System.out.println("No files found matching the pattern.");
        // }
    }

    @Test
    public void testCreateUser() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testCreateUser", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testCreateUserAlreadyExists() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testCreateUserAlreadyExists", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to create a new user with same username as existing user
            response = kvClient.createUser("testCreateUserAlreadyExists", "password2");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_ERROR, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testLogin() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLogin", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testLogin", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testLoginWithWrongPassword() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLoginWithWrongPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testLoginWithWrongPassword", "password2");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_ERROR, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testLoginOnAnotherServer() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLoginOnAnotherServer", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Connect to different server
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50003);
            kvClient.connect();

            // Attempt to login
            response = kvClient.login("testLoginOnAnotherServer", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }  

    @Test
    public void testLogout() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLogout", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testLogout", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());

            // Attempt to logout
            response = kvClient.logout();
            assertNotNull(response);
            assertEquals(StatusType.LOGOUT_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testPutGetWhileLoggedIn() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testPutGetWhileLoggedIn", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testPutGetWhileLoggedIn", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());

            // Attempt to put key value pair
            response = kvClient.put("keytestPutGetWhileLoggedIn", "value");
            assertNotNull(response);
            assertEquals(StatusType.PUT_SUCCESS, response.getStatus());

            // Attempt to get key value pair
            response = kvClient.get("keytestPutGetWhileLoggedIn");
            assertNotNull(response);
            assertEquals(StatusType.GET_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testResetPassword() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());

            // Attempt to reset password
            response = kvClient.resetPassword("testResetPassword", "passwordNEW");
            assertNotNull(response);
            assertEquals(StatusType.RESET_PASSWORD_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testLoginAfterResetPassword() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLoginAfterResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testLoginAfterResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());

            // Attempt to reset password
            response = kvClient.resetPassword("testLoginAfterResetPassword", "passwordNEW");
            assertNotNull(response);
            assertEquals(StatusType.RESET_PASSWORD_SUCCESS, response.getStatus());

            // Attempt to logout
            response = kvClient.logout();
            assertNotNull(response);
            assertEquals(StatusType.LOGOUT_SUCCESS, response.getStatus());

            // Attempt to login with new password
            response = kvClient.login("testLoginAfterResetPassword", "passwordNEW");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }

    @Test
    public void testLoginOnAnotherServerAfterResetPassword() {
        try {
            // Attempt to create a new user
            KVMessage response = kvClient.createUser("testLoginOnAnotherServerAfterResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.CREATE_SUCCESS, response.getStatus());

            // Attempt to login
            response = kvClient.login("testLoginOnAnotherServerAfterResetPassword", "password");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());

            // Attempt to reset password
            response = kvClient.resetPassword("testLoginOnAnotherServerAfterResetPassword", "passwordNEW");
            assertNotNull(response);
            assertEquals(StatusType.RESET_PASSWORD_SUCCESS, response.getStatus());

            // Connect to different server
            kvClient.disconnect();
            kvClient = new KVStore("localhost", 50003);
            kvClient.connect();

            // Attempt to login with new password
            response = kvClient.login("testLoginOnAnotherServerAfterResetPassword", "passwordNEW");
            assertNotNull(response);
            assertEquals(StatusType.LOGIN_SUCCESS, response.getStatus());
        } catch (Exception e) {
        }
    }
}