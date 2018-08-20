
exports.subscribe = function (event, callback) {

  // Imports the Google Cloud client library
  const Datastore = require('@google-cloud/datastore');

  // Your Google Cloud Platform project ID
  const projectId = 'celestial-brand-212615';
  const PubSubMessage = event.data;
  const incomingData = PubSubMessage.data ? Buffer.from(PubSubMessage.data, 'base64').toString() : "";

  const jsonData = JSON.parse(incomingData);
  var rows = [jsonData];

  //ignore data that's not from today
  const dateToday = new Date();
  const updateDate = new Date(jsonData._last_updt);

  if(dateToday.getDay() === updateDate.getDay()) {

    console.log(`Uploading data: ${JSON.stringify(rows)}`);

    // Creates a client
    const datastore = new Datastore({
      projectId: projectId,
    });

    // The kind for the new entity
    const kind = 'segment';

    // The name/ID for the new entity
    const name = jsonData.segmentid;

    // The Cloud Datastore key for the new entity
    const taskKey = datastore.key([kind, name]);

    const lif = {
       latitude: jsonData._lif_lat,
       longitude: jsonData.start_lon
    };

    const lit = {
       latitude: jsonData._lit_lat,
       longitude: jsonData._lit_lon
    };

    const geoLif = datastore.geoPoint(lif);
    const geoLit = datastore.geoPoint(lit);

    // Prepares the new entity
    const task = {
      key: taskKey,
      data: {
        _direction: jsonData._direction,
        _fromst: jsonData._fromst,
        _last_updt: new Date(jsonData._last_updt),
        _length: parseFloat(jsonData._length),
        _lif: geoLif,
        _lit: geoLit,
        _strheading: jsonData._strheading,
        _tost: jsonData._tost,
        _traffic: parseInt(jsonData._traffic),
        segmentid: jsonData.segmentid,
        street: jsonData.street
       },
      };

    // Saves the entity
    datastore
      .save(task)
      .then(() => {
        console.log(`Saved ${task.key.name}`);
      })
      .catch(err => {
        console.error('ERROR:', err);
      });

  } else {
    console.log(`update date not from today, ignoring.`);
  }
  callback();
}