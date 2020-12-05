const MongoClient = require("mongodb").MongoClient;
const processLib = require("process");
import * as assert from "assert";

namespace postmortem_db {
  export class PostmortemDb {
    uri: string;
    incident: string;

    constructor(uri: string, incident: string) {
      this.uri = uri;
      this.incident = incident;
    }

    async init() {
      console.log("Starting...");
      try {
          const res = await this.readOrCreateIncident();
          console.log("Records: " + res);
      } catch (error) {
          console.log(error);
      }
      console.log("Done!");
    }

    async readOrCreateIncident(): Promise<any[]> {
      console.log("Check incident " + this.incident);
      const db = await MongoClient.connect(this.uri);
      const dbo = db.db(this.incident);
      const coll = dbo.collection("meta");
      var result = await coll.find({}).toArray();

      if (result.length > 0) {
        return result;
      }
      console.log('Create new Meta reacord');
      result = await coll.insertOne({
                  incident: this.incident,
                });
      return result;
    }
  }
} // namespace

export { postmortem_db };
