

  // Imports the Google Cloud client library
  const Datastore = require('@google-cloud/datastore');

  // Your Google Cloud Platform project ID
  const projectId = 'celestial-brand-212615';

  const incomingData = '{"_direction": "EB", "_fromst": "California", "_last_updt": "2018-08-20 11:50:58.0", "_length": "0.48", "_lif_lat": "42.0119763841", "_lit_lat": "42.0121628852", "_lit_lon": "-87.6902422854", "_strheading": "W", "_tost": "Western", "_traffic": "-1", "segmentid": "1002", "start_lon": "-87.6997466265", "street": "Touhy"}';

  const jsonData = JSON.parse(incomingData);
  var rows = [jsonData];

  //ignore data that's not from today
  const dateToday = new Date();
  const updateDate = new Date(jsonData._last_updt);

  if(dateToday.getDay() === updateDate.getDay()) {

//  console.log(`Uploading data: ${JSON.stringify(rows)}`);
//
  const dtoday = new Date();
    const bustoday = new Date('2018-08-20 13:31:13.0');
    bustoday.setUTCDate(-6);

    console.log(`days the same?: ${dtoday.getDay() === bustoday.getDay()}`);
    console.log(`bus date: ${bustoday}`);

//  const maybe = new Date('2018-08-20 11:31:17.0');
//  const gdate = new Date('2018-08-20T13:40:04Z');
//
//  console.log(`dates the same?: ${dtoday === nottoday}`);
//  console.log(`days the same?: ${dtoday.getDay() === nottoday.getDay()}`);
//
//  console.log(`maybe days the same?: ${dtoday.getDay() === maybe.getDay()}`);
//  console.log(`gdate days the same?: ${dtoday.getDay() === gdate.getDay()}`);




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