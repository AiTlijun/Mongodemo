package com.mongo.demo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.client.model.Indexes;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Filters.eq;
import static java.util.Arrays.asList;

public class MongoDBExamples {

    public static void main(final String[] args) {

        // 1. Connect to MongoDB instance running on localhost
        MongoClient mongoClient = new MongoClient();
        // Access database named 'test'
        MongoDatabase database = mongoClient.getDatabase("test");

        // Access collection named 'restaurants'
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


        // 3. Query
        List<Document> results = collection.find().into(new ArrayList<>());


        // 4. Create Index
        collection.createIndex(Indexes.ascending("name"));
        // 5. Perform Aggregation
        collection.aggregate(asList(match(eq("categories", "Bakery")),
                group("$stars", sum("count", 1))));


        mongoClient.close();

    }

}