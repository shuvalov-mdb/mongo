const mongodb = require("mongodb");
const processLib = require("process");
const fs = require("fs");
const path = require("path");
import * as assert from "assert";
import { db_spawner } from "./db_spawner";
import { MongoClient, Db } from "mongodb";

namespace postmortem_db {
  export class PostmortemDb {
    uri: string;
    incident: string;
    path: string;
    db: MongoClient;
    incidentDb: Db;

    constructor(uri: string, incident: string) {
      this.uri = uri;
      this.incident = incident;
      this.path = "";
    }

    setPath(path: string) {
      this.path = path;
    }

    async init() {
      console.log("Starting...");
      this.db = await MongoClient.connect(this.uri, {
        useUnifiedTopology: true,
      });
      var metadata = await this.readOrCreateIncident();
      console.log("Records: " + JSON.stringify(metadata));

      var dataLoadResult = await this.loadDataIfNeeded();
      console.log("Data load: " + JSON.stringify(dataLoadResult));
    }

    async readOrCreateIncident(): Promise<JSON> {
      console.log("Check incident " + this.incident);
      this.incidentDb = this.db.db(this.incident);
      const coll = this.incidentDb.collection("meta");
      var result = await coll.findOne({ incident: this.incident });

      if (result) {
        this.path = result.path;
        console.log("Incident path " + this.path);
        return result;
      }
      if (!this.path)
        throw "--path is required to init the Metadata for the first time";
      console.log("Create new Meta reacord");
      var insertResult = await coll.insertOne({
        incident: this.incident,
        path: this.path,
      });
      var meta = await coll.findOne({ incident: this.incident });
      if (meta == null) throw 'Write failed';
      return meta;
    }

    async loadDataIfNeeded(): Promise<JSON> {
      console.log("Check if data needs to be loaded...");
      var dataPath = path.join(this.path, "data/db/job0/resmoke/");
      for (const replicaSet of fs.readdirSync(dataPath)) {
          for (const node of fs.readdirSync(path.join(dataPath, replicaSet))) {
            console.log(
              "Checking node " + node + " for replica set " + replicaSet
            );
            var result = await this.loadDataForNodeIfNeeded(
              replicaSet,
              node,
              path.join(dataPath, replicaSet, node)
            );
            console.log(result);
        }
      }
      return <JSON>{};
    }

    async loadDataForNodeIfNeeded(
      replicaSet: string,
      node: string,
      path: string
    ): Promise<JSON> {
      try {
        const coll = this.incidentDb.collection("meta");
        var docCursor = await coll.find({
          replicaSet: replicaSet,
          node: node,
        });
        if ((await docCursor.count()) !== 0) {
          var docs = docCursor.toArray();
          console.log('Already loaded ' + JSON.stringify(docs));
          // return JSON.stringify(docs[0]);
          return <JSON>{};
        }
        var loadReasult = await this.loadDataForNode(replicaSet, node, path);
      } finally {
        return <JSON>{};
      }
    }

    async loadDataForNode(
      replicaSet: string,
      node: string,
      path: string
    ): Promise<JSON> {
      var spawner = new db_spawner.DbSpawner(path, replicaSet + "_" + node);
      await spawner.spawn();
      return <JSON>{};
    }
  }
} // namespace

export { postmortem_db };
