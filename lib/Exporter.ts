import * as Debug from "debug";
const debug = Debug("mpli:bqexport");

import * as BigQuery from "@google-cloud/bigquery";
import * as async from "async";
import * as uuid from "uuid";

import ConfigInterface from "./interfaces/ConfigInterface";
import FlatResultInterface from "./interfaces/FlatResultInterface";
import { ResultsetInterface, ResultsetAdvancedInterface, TestInterface } from "@metrics-pli/types";

const DEFAULT_SCHEMA = {
    schema: {
        fields: [
            { name: "score", type: "INTEGER", mode: "NULLABLE" },
            { name: "raw_value", type: "INTEGER", mode: "NULLABLE" },
            { name: "name", type: "STRING", mode: "NULLABLE" },
            { name: "type", type: "STRING", mode: "NULLABLE" },
            { name: "url", type: "STRING", mode: "NULLABLE" },
            { name: "tested_at", type: "TIMESTAMP", mode: "NULLABLE" },
        ],
    },
    timePartitioning: { type: "DAY" },
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

    constructor(config: ConfigInterface) {
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

    private ensureDatasetExists(callback: (error?: Error) => void): void {

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

    private checkTableExists(callback: (error?: Error) => void): void {

        this.table.exists((error, exists) => {

            if (error) {
                return callback(new Error(`Failed to check if table exists: ${JSON.stringify(error)}`));
            }

            this.initializedTable = exists;
            callback();
        });
    }

    private ensureTableExists(callback: (error?: Error) => void): void {

        if (this.initializedTable) {
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
            },
        );
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
                    .catch((error) => next(error)),
                (error) => {
                    this.bufferDraining = false;

                    if (error) {
                        return reject(error);
                    }

                    resolve();
                },
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

        if (Date.now() >= this.lastBufferFlush + this.config.batchTimeout) {
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

    private mapResultToFlatRows(result: ResultsetAdvancedInterface, test: TestInterface): FlatResultInterface[] {
        return result.audits.map((audit) => {

            let rawValue: any = audit.rawValue;
            let score: any = audit.score;

            if (typeof rawValue !== "number") {
                rawValue = parseInt(rawValue, 10);
                if (isNaN(rawValue)) {
                    rawValue = -1;
                }
            }

            if (typeof score !== "number") {
                score = parseInt(score, 10);
                if (isNaN(score)) {
                    score = -1;
                }
            }

            const flattened: FlatResultInterface = {
                score,
                raw_value: rawValue,
                name: test.name,
                type: audit.id,
                url: test.url,
                tested_at: new Date().toISOString(),
            };

            return flattened;
        });
    }

    public async init() {

        debug("Initiating..");

        this.bufferInterval = setInterval(this.onBufferInterval.bind(this), 1000);

        await async.series(
            [
                (done) => this.ensureDatasetExists(done),
                (done) => this.checkTableExists(done),
                (done) => this.ensureTableExists(done),
            ],
        );

        debug("Initiation done.");
    }

    public registerWith(metricsPli: any): void {

        metricsPli.on("data", (event) => {
            const {result, test}: {result: ResultsetInterface, test: TestInterface} = event;

            if (!result.advanced) {
                return;
            }

            const advanced: ResultsetAdvancedInterface = result.advanced;
            debug("Received metrics for", result.advanced.url);

            this.putRecords(this.mapResultToFlatRows(advanced, test)).catch((error) => {
                // proxy error
                metricsPli.emit("error", error);
            });
        });

        debug("Registered with metricsPli instance.");
    }

    public putRecords(records: FlatResultInterface[]): Promise<void> {

        records.forEach((record) => {

            const row = {
                insertId: uuid.v4(),
                json: record,
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

        if (this.bufferInterval) {
            clearInterval(this.bufferInterval);
        }

        await this.drainBuffer();
        debug("Closed.");
    }
}
