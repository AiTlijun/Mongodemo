package com.mongo.demo;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;

public class MongoDBJDBC {
    public static void main(String[] args) {
        try {
            //连接到MongoDB服务 如果是远程连接可以替换“localhost”为服务器所在IP地址
            //ServerAddress()两个参数分别为 服务器地址 和 端口
            ServerAddress serverAddress;
            serverAddress = new ServerAddress("132.232.182.241", 27017);
            List<ServerAddress> addrs = new ArrayList<ServerAddress>();
            addrs.add(serverAddress);

            //MongoCredential.createScramSha1Credential()三个参数分别为 用户名 数据库名称 密码
            //MongoCredential credential = MongoCredential.createScramSha1Credential("lijun", "test", "123456".toCharArray());
            MongoCredential credential = MongoCredential.createCredential("test_usr", "test", "test_pwd".toCharArray());
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(credential);

            //通过连接认证获取MongoDB连接
            MongoClient mongoClient = new MongoClient(addrs, credentials);

            //连接到数据库
            MongoDatabase mongoDatabase = mongoClient.getDatabase("test");
            System.out.println("Connect to database successfully");
//            /insertDOC(mongoClient, mongoDatabase);
           // query(mongoClient, mongoDatabase);
            //createIndex(mongoClient, mongoDatabase);
            aggregate(mongoClient, mongoDatabase);
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
        }

    }

    public static void insertDOC(MongoClient mongoClient, MongoDatabase database) {
        // Access collection named 'restaurants'
        database.createCollection("restaurants");
        MongoCollection<Document> collection = database.getCollection("restaurants");

        // 2. Insert
        List<Document> documents = asList(
                new Document("name", "Sun Bakery Trattoria")
                        .append("stars", 4)
                        .append("categories", asList("Pizza", "Pasta", "Italian", "Coffee", "Sandwiches")),
                new Document("name", "Blue Bagels Grill")
                        .append("stars", 3)
                        .append("categories", asList("Bagels", "Cookies", "Sandwiches")),
                new Document("name", "Hot Bakery Cafe")
                        .append("stars", 4)
                        .append("categories", asList("Bakery", "Cafe", "Coffee", "Dessert")),
                new Document("name", "XYZ Coffee Bar")
                        .append("stars", 5)
                        .append("categories", asList("Coffee", "Cafe", "Bakery", "Chocolates")),
                new Document("name", "456 Cookies Shop")
                        .append("stars", 4)
                        .append("categories", asList("Bakery", "Cookies", "Cake", "Coffee")));

        collection.insertMany(documents);

        mongoClient.close();

    }

    public static void query(MongoClient mongoClient, MongoDatabase database) {
        MongoCollection<Document> collection = database.getCollection("restaurants");
        // 3. Query
        List<Document> results = collection.find().into(new ArrayList<>());
        //System.out.println(results.forEach());
        results.forEach(System.out::println);

        mongoClient.close();

    }

    public static void createIndex(MongoClient mongoClient, MongoDatabase database) {
        MongoCollection<Document> collection = database.getCollection("restaurants");
        // 3. createIndex
        String index = collection.createIndex(Indexes.ascending("name"));
        System.out.println("----createIndex--->"+index);
        mongoClient.close();

    }

    public static void aggregate(MongoClient mongoClient, MongoDatabase database) {
        MongoCollection<Document> collection = database.getCollection("restaurants");
        // 4. aggregate
        AggregateIterable<Document> aggregate = collection.aggregate(asList(match(eq("categories", "Bakery")),
                group("$stars", sum("count", 1))));
        //aggregate.forEach(System.out::println());
        MongoCursor<Document> iterator = aggregate.iterator();
        Document document = iterator.tryNext();
        System.out.println(document.toString());
        mongoClient.close();

    }
}
