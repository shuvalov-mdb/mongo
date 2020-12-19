const mongodb = require("mongodb");
const { spawn } = require("child_process");
const getPort = require('get-port');
const fs = require('fs');
const path = require("path");
import { MongoClient, Db, Cursor } from "mongodb";

namespace db_spawner {
  export class DbSpawner {
    static nextPortRange: number = 27018;  // To avoid using same port.
    dataPath: string;
    id: string;
    port: number;
    db: MongoClient;
    adminDb: Db;

    constructor(dataPath: string, id: string) {
      this.dataPath = dataPath;
      this.id = id;
      this.port = -1;
    }

    async spawn() {
      this.port = await getPort({port: getPort.makeRange(DbSpawner.nextPortRange, 27099)});
      DbSpawner.nextPortRange++;
      console.log('Spawn DB on port ' + this.port + ', data path ' + this.dataPath + ', log /tmp/' + this.id + '.log');
      try {
        fs.unlinkSync(path.join('/tmp', this.id + ".log"));
      } catch(err) {}
      const spawned = spawn("mongod", [
        "--dbpath",
        this.dataPath,
        "--setParameter",
        "recoverFromOplogAsStandalone=true",
        //"--fork",
        "--port",
        this.port,
        "--logpath",
        "/tmp/" + this.id + ".log",
      ]);

      spawned.stdout.on("data", (data: string) => {
        console.log(`stdout: ${data}`);
      });

      spawned.stderr.on("data", (data: string) => {
        console.error(`stderr: ${data}`);
      });

      spawned.on("close", (code: number) => {
        console.log(`child process exited with code ${code}`);
      });
    }

    async connect(): Promise<JSON[]> {
      var uri = `mongodb://localhost:${this.port}`;
      console.log('Connect to ' + uri);
      this.db = await MongoClient.connect(uri, {
        useUnifiedTopology: true,
      });
      this.adminDb = this.db.db("admin");
      return [<JSON>{}];
    }

    async listDbs(): Promise<string[]> {
      var result = await this.adminDb.command({ listDatabases: 1 });
      console.log(result);
      var dbs: string[] = [];
      for (var i = 0; i < result.databases.length; i++) {
        var name = result.databases[i]['name'];
        if (!['admin', 'config', 'local'].includes(name)) {
          dbs.push(name);
        }
      }
      return dbs;
    }

    async listCollections(dbName: string): Promise<string[]> {
      var db = this.db.db(dbName);
      var colls: string[] = [];
      var collections = await db.listCollections({}).toArray();
      for (var i = 0; i < collections.length; i++) {
        colls.push(collections[i]["name"]);
      }
      return colls;
    }

    async loadData(dbName: string, collName: string): Promise<Cursor<any>> {
      const dataDb = this.db.db(dbName);
      const coll = dataDb.collection(collName);
      return coll.find({});
    }

    async shutdown(): Promise<JSON[]> {
      var result = await this.adminDb.command({
        shutdown : 1
      });
      return result;
    }
  }
} // namespace

export { db_spawner };
