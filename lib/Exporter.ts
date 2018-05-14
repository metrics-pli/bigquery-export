import * as Debug from "debug";
const debug = Debug("mpli:bqexport");

import * as BigQuery from "@google-cloud/bigquery";
import * as async from "async";
import * as uuid from "uuid";

import ConfigInterface from "./interfaces/ConfigInterface";
import RecordInterface from "./interfaces/RecordInterface";

const DEFAULT_SCHEMA = {

};

export default class Exporter {

    private config: ConfigInterface;
    private bigQuery: any;
    private dataset: any;
    private table: any;
    private initializedTable: any;
    private buffer: any[];
    private bufferDraining: boolean;
    private bufferInterval: any;
    private lastBufferFlush: number;

    constructor(config: ConfigInterface){
        this.config = config;

        this.bigQuery = new BigQuery(this.config.bigquery);
        this.dataset = this.bigQuery.dataset(this.config.dataset);
        this.table = this.dataset.table(this.config.table);
        this.initializedTable = null;
        this.buffer = [];
        this.bufferDraining = false;
        this.bufferInterval = null;
        this.lastBufferFlush = Date.now();
    }

    private ensureDatasetExists(callback: Function): void {

        this.dataset.exists((error, exists) => {

            if (error) {
                return callback(new Error(`Failed to check if dataset exists: ${JSON.stringify(error)}`));
            }

            if (!exists) {
                return this.dataset.create(callback);
            }

            callback();
        });
    }

    private checkTableExists(callback: Function): void {

        this.table.exists((error, exists) => {

            if (error) {
                return callback(new Error(`Failed to check if table exists: ${JSON.stringify(error)}`));
            }

            this.initializedTable = exists;
            callback();
        });
    }

    private ensureTableExists(callback: Function): void {

        if(this.initializedTable){
            debug("Table already exists.");
            return callback();
        }

        this.table.create(DEFAULT_SCHEMA, (error, table, apiResponse) => {

                if (error) {

                    // This might happen if multiple instances of the exporter try to
                    // create the table concurrently. This is a simple remedy in case
                    // there is no external locking functionality available. The error
                    // is simply swallowed.
                    if (error.code === 409 && error.message.startsWith("Already Exists:")) {
                        return callback();
                    }

                    debug("Failed to create table", error.message);
                    return callback(error);
                }

                debug("Created table");
                this.initializedTable = table;
                callback();
            }
        )
    }

    private drainBuffer(): Promise<void> {

        if (this.bufferDraining) {
            return Promise.resolve();
        }

        this.lastBufferFlush = Date.now();
        this.bufferDraining = true;

        return new Promise((resolve, reject) => {
            async.whilst(
                () => this.buffer.length !== 0,
                (next) => this.runBatch()
                    .then(() => next())
                    .catch(error => next(error)),
                (error) => {
                    this.bufferDraining = false;

                    if (error) {
                        return reject(error);
                    }

                    resolve();
                }
            );
        });
    }

    private runBatch(): Promise<void> {

        const rows = this.buffer.splice(0, Math.min(this.config.batchSize, this.buffer.length));
        const options = { raw: true };

        return new Promise((resolve, reject) => this.table.insert(rows, options, (error, apiResponse) => {

            if (error) {
                debug("Failed to dispatch", rows.length, "to BigQuery", error.message);
                this.buffer.splice(0, 1, ...rows);
                return reject(error);
            }

            debug("Dispatched", rows.length, "rows to BigQuery.");
            resolve();
        }));
    }

    private onBufferInterval(): void {
        
        if(Date.now() >= this.lastBufferFlush + this.config.batchTimeout){
            return;
        }

        this.drainBuffer()
            .then(() => {
                debug("Drained buffer from buffer interval");
            })
            .catch((error) => {
                debug("Failed to drain buffer from buffer interval", error.message);
            });
    }

    public async init(){

        debug("Initiating..");

        this.bufferInterval = setInterval(this.onBufferInterval.bind(this), 1000);

        await async.series(
            [
                done => this.ensureDatasetExists(done),
                done => this.checkTableExists(done),
                done => this.ensureTableExists(done)
            ]
        );

        debug("Initiation done.");
    }

    public registerWith(metricsPli: any): void {

        metricsPli.on("data", ({ result, test }) => {
            debug("Received metrics", test);
            this.putRecords([result]).catch((error) => {
                // proxy error
                metricsPli.emit("error", error);
            });
        });

        debug("Registered with metricsPli instance.");
    }

    public putRecords(records: RecordInterface[]): Promise<void> {

        records.forEach(record => {

            const row = {
                insertId: uuid.v4(),
                json: record
            };
            
            this.buffer.push(row);
        });

        if (this.buffer.length >= this.config.batchSize) {
            return this.drainBuffer();
        }

        return Promise.resolve();
    }

    public async close(): Promise<void> {

        debug("Closing..");

        if(this.bufferInterval){
            clearInterval(this.bufferInterval);
        }

        await this.drainBuffer();

        debug("Closed.");
    }
}
