const mongodb = require("mongodb");
const { spawn } = require("child_process");
const getPort = require('get-port');
import { MongoClient, Db } from "mongodb";

namespace db_spawner {
  export class DbSpawner {
    dataPath: string;
    id: string;
    port: number;

    constructor(dataPath: string, id: string) {
      this.dataPath = dataPath;
      this.id = id;
      this.port = -1;
    }

    async spawn() {
      this.port = await getPort({port: getPort.makeRange(27018, 27099)});
      console.log('Spawn DB on port ' + this.port + ', data path ' + this.dataPath);
      const spawned = spawn("mongod", [
        "--dbpath",
        this.dataPath,
        "--setParameter",
        "recoverFromOplogAsStandalone=true",
        "--fork",
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

    async loadData(): Promise<JSON[]> {
      const db = await MongoClient.connect("mongodb://localhost:${this.port}", {
        useUnifiedTopology: true,
      });
      const dataDb = db.db("");
      const coll = dataDb.collection("meta");

      return [<JSON>{}];
    }

    async shutdown(): Promise<JSON[]> {
      MongoClient.connect("mongodb://localhost:27017/admin", function(err, db) {
        if (err) {
          console.log('mongodb is not running');
          process.exit(0);
        }
        else {
          db.command({
            shutdown : 1
          }, function(err, result) {
            console.log('shutting down mongodb - ', err.message);
            process.exit(0);
          });
        }
      });
    }
  }
} // namespace

export { db_spawner };
