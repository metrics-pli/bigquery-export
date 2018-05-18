# bigquery-export

Exports collected metrics to Google Big Query
Can be hooked up very easily to a metricsPli instance.

You can debug logs via `DEBUG=mpli:bqexport`.
The bigquery config fields accepts all parameters that you can pass to
`@google-cloud/bigquery`.

```javascript
import MetricsPli, { ConfigInterface, TestInterface } from "@metrics-pli/core";
import Exporter from "@metrics-pli/exporter-bigquery";

const exporterConfig = {
    bigquery: {
        projectId: "123123123",
    },
    dataset: "my_metrics_dataset",
    table: "my_metrics_table",
    batchSize: 10,
    batchTimeout: 60 * 1000
};

const tests: TestInterface[] = [{
  name: "Homepage",
  url: "https://google.com/",
}];

const config: ConfigInterface = {};

(async () => {
  const metricsPli = new MetricsPli(tests, config);
  const bqExporter = new Exporter(exporterConfig);

  await bqExporter.init();
  bqExporter.registerWith(metricsPli);

  // will also emit exporter error events
  metricsPli.on("error", console.error);
  metricsPli.on("info", console.info);
 
  await metricsPli.run();
  await bqExporter.close();
})();
```