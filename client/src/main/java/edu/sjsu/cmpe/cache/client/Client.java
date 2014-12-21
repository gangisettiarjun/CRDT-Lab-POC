package edu.sjsu.cmpe.cache.client;

public class Client {

    public static void main(String[] args) throws Exception {
        System.out.println("Starting Cache Client...");

        ReplicationClient rct = new ReplicationClient();
        rct.addServer("http://localhost:3000");
        rct.addServer("http://localhost:3001");
        rct.addServer("http://localhost:3002");
        rct.put(1, "a");
        Thread.sleep(30*1000);
        rct.put(1, "b");
        Thread.sleep(30*1000);
        System.out.println("Values in Cache Servers: "+rct.get(1));
        System.out.println("Existing Cache Client...");
    }

}
