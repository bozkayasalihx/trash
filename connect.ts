import { MongoClient } from "mongodb";

export async function connectToMongo() {
    const client = new MongoClient("mongodb://localhost:27017");
    await client.connect();
    return client.db("results").collection<any>("aggragatedResults");
}
