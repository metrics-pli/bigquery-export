export default interface ConfigInterface {
    bigquery: {
        projectId: string;
    };
    dataset: string;
    table: string;
    batchSize: number;
    batchTimeout: number;
}
