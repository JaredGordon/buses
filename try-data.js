/**
 * Background Cloud Function to be triggered by PubSub.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */

  const BigQuery = require('@google-cloud/bigquery');
  const projectId = "celestial-brand-212615"; //Enter your project ID here
  const datasetId = "buses"; //Enter your BigQuery dataset name here
  const tableId = "buses"; //Enter your BigQuery table name here -- make sure it is setup correctly

  // Incoming data is in JSON format
  const incomingData = '{"_direction": "EB", "_fromst": "California", "_last_updt": "2011-10-28 11:50:50.0", "_length": "0.48", "_lif_lat": "42.0119763841", "_lit_lat": "42.0121628852", "_lit_lon": "-87.6902422854", "_strheading": "W", "_tost": "Western", "_traffic": "-1", "segmentid": "1002x", "start_lon": "-87.6997466265", "street": "Touhy"}';
  const jsonData = JSON.parse(incomingData);
  const x = [jsonData]

  console.log(`Uploading data: ${JSON.stringify(x)}`);

  // Instantiates a client
  const bigquery = BigQuery({
    projectId: projectId
  });

 // const key = jsonData.segmentid + jsonData._last_updt;
 const key = 'foo'
//  const data = [jsonData];

  const rows = [{
    insertid: key,
    json: x
  }];

  // Inserts data into a table
  bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert(rows)
      .then(() => {
        console.log(`Saved ${jsonData.segmentid}`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });
  // [END bigquery_insert_stream]
