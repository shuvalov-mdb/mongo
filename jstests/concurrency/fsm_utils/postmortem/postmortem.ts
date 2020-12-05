// Run this script as:
//   npm run postmortem -- --url localhost:27017

const chalk = require('chalk');
const processLib = require('process')
import { postmortem_db } from "./postmortem_db";

var argv = require('minimist')(process.argv.slice(2));

function help() {
    console.log(`Help:
      --url: MongoDB connection
      --incident: ID of the incident to use
      `);
}

if (!argv.url || argv.help || !argv.incident) {
    help();
    processLib.exit(1);
}

let db = new postmortem_db.PostmortemDb(argv['url'], argv['incident']);

db.init()
    .then(text => {
        console.log(text);
        processLib.exit(0);
    })
    .catch(err => {
        console.log(err);
        processLib.exit(1);
    });

