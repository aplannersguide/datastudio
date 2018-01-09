var moment = Moment.load();

function getConfig(request) {
  var config = {
    configParams: [
      {
        type: "INFO",
        name: "connect",
        text: "PLEASE NOTE: This connector requires and domain and dataset ID to get started."
      },
      {
        type: 'TEXTINPUT',
        name: 'domain',
        displayName: 'Domain',
        helpText: 'Copy and paste the Domain (e.g. https://opendata.socrata.com).',
        placeholder: 'https://opendata.socrata.com'
      },
      {
        type: 'TEXTINPUT',
        name: 'id',
        displayName: 'Dataset ID',
        helpText: 'Copy and paste the dataset ID (e.g. 1234-abcd)',
        placeholder: '4jxs-y9s7'
      },
      {
        type: "INFO",
        name: "dyndate",
        text: "You can add a dynamic date filter to this data source where, on data refresh, it will only retrieve data based on criteria you set. It requires that your dataset contain a date column."
      },
      {
        type: 'CHECKBOX',
        name: 'usedynamicdate',
        displayName: 'Dynamic Date Filtering?',
        helpText: 'Use Dynamic Date Filtering?'
      },
      {
        type: 'TEXTINPUT',
        name: 'datecol',
        displayName: 'Date Column',
        helpText: 'Enter a date column for dynamoic filtering',
        placeholder: 'start_date'
      },
      {
        type: 'TEXTINPUT',
        name: 'timeago',
        displayName: 'Time Ago:',
        helpText: 'The integer for how many days/months/years in the past to pull',
        placeholder: '1'
      },
      {
        type: 'SELECT_SINGLE',
        name: 'timeframe',
        displayName: 'Timeframe',
        options: [
        {
          "label": "Years",
          "value": "years"
        },
        {
          "label": "Months",
          "value": "months"
        },
        {
          "label": "Days",
          "value": "days"
        }
      ]
     },
            {
        type: 'SELECT_SINGLE',
        name: 'upto',
        displayName: 'Up to',
        options: [
        {
          "label": "Today",
          "value": "days"
        },
        {
          "label": "Last Month",
          "value": "months"
        }
      ]
     }
        
    ]
  };
  return config;
};

function toTableSchema(schemaRow) {
    return schemaRow.map(toField);
}

function toField(tableSchemaField) {
    switch (tableSchemaField.dataTypeName) {
        case 'boolean':
            ftype = 'BOOLEAN';
            format = 'text';
            break;
        case 'number':
        case 'money' :
        case 'integer':
            ftype = 'NUMBER';
            break;
        default:
            ftype = 'STRING';
    }

    return {
        'name': tableSchemaField.fieldName,
        'label': tableSchemaField.name,
        'dataType': ftype
    }
}
function schemaInit(domain, ID) {
  var c = [domain, "/api/views/", ID, "/columns.json"];
  var url = c.join("");
  var response = JSON.parse(UrlFetchApp.fetch(url));
  return response;
}

function dataDynamicInit(domain, ID, api_fields, api_query) {
  
  // Fetch total number of rows
  query = ["$select=count(*) as count&$where=",[api_query[0], ">='",api_query[1],"' and ",api_query[0]," <='", api_query[2], "'"].join("")].join("");
  var c = [domain, "/resource/", ID, ".json?", query];
  
  var url = encodeURI(c.join(""));
  JSON.parse(UrlFetchApp.fetch(url, {"escaping":false}))
  var count = parseInt(JSON.parse(UrlFetchApp.fetch(url))[0]["count"]);

  // Iterate through and collect data
  data = [];
  for(var i = 0; i < count; i += 50000) {
    if(i === 0) {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$select=", api_fields.join(","),"&$where=", api_query[0],">='",api_query[1],"' and ",api_query[0]," <='", api_query[2], "'"];
    } else {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$offset=", i.toString(), "&$select=", api_fields.join(","),"&$where=", api_query[0],">='",api_query[1],"' and ", api_query[0], "<='", api_query[2], "'"];
    }
    url = encodeURI(c.join(""));
    var j = UrlFetchApp.fetch(url);
    var d = JSON.parse(UrlFetchApp.fetch(url));
    data.push.apply(data, d);
  }
  return data;
}

function dataInit(domain, ID, api_fields) {
  
  // Fetch total number of rows
  query = "$select=count(*) as count";
  var c = [domain, "/resource/", ID, ".json?", query];
  
  var url = encodeURI(c.join(""));

  JSON.parse(UrlFetchApp.fetch(url, {"escaping":false}))
  var count = parseInt(JSON.parse(UrlFetchApp.fetch(url))[0]["count"]);

  // Iterate through and collect data
  data = [];
  for(var i = 0; i < count; i += 50000) {
    if(i === 0) {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$select=", api_fields.join(",")];
    } else {
      c = [domain, "/resource/", ID, ".json?$limit=50000&$offset=", i.toString(), "&$select=", api_fields.join(",")];
    }
    url = encodeURI(c.join(""));
    var j = UrlFetchApp.fetch(url);
    var d = JSON.parse(UrlFetchApp.fetch(url));
    data.push.apply(data, d);
  }
  return data;
}

function getSchema(request) {
    var domain = request.configParams.domain;
    var datasetID = request.configParams.id;
    var tableSchema = toTableSchema(schemaInit(domain, datasetID));
    return {'schema': tableSchema};
}

function toRowResponse(fieldNames, row) {
    return {
        'values': fieldNames.map(function (field) {
          if(field.dataType === "number") {
            return parseInt(row[field.name]);
          } else {
            return row[field.name]; 
          }
        })
    };
}

function getData(request) {
  var domain = request.configParams.domain;
  var datasetID = request.configParams.id;

  var dataSchema = [];
  var socrataSchema = schemaInit(domain, datasetID);
  var tableSchema = toTableSchema(socrataSchema);
  var api_fields = [];
  var formatted_fields = [];
  var api_query = [];
  var unmappedData = [];
  for(var i = 0; i < tableSchema.length; i++) {
    request.fields.forEach(function(user) {
        if (user.name === tableSchema[i].name) {
            dataSchema.push(tableSchema[i]);
            api_fields.push(user.name);
          if(tableSchema[i].dataType === "number") {
            formatted_fields.push({"name":user.name, "format":"number"});
          }
          if(socrataSchema[i].dataTypeName === "calendar_date") {
            formatted_fields.push({"name":user.name, "format":"date"});
          }
        }
    });
   }
  
  if(request.configParams.usedynamicdate === "true") {
    var timeago = request.configParams.timeago;
    var datecol = request.configParams.datecol;
    var timeframe = request.configParams.timeframe;
    var uptoCount = request.configParams.upto === "" ? 0 : 1;
    var upto = request.configParams.upto === "" ? "days" : request.configParams.upto;
    
    var dynamicDate = moment().subtract(parseInt(timeago), timeframe).format("YYYY-MM-DD");
    var uptoDate = moment().subtract(uptoCount, upto).format("YYYY-MM-DD"); 
    api_query = [datecol, dynamicDate, uptoDate];
    unmappedData = dataDynamicInit(domain, datasetID, api_fields, api_query);
  } else {
    unmappedData = dataInit(domain, datasetID, api_fields);
  }
  
  
  
  var data = unmappedData.map(function(row) {
    return toRowResponse(dataSchema, row);
  });
  return {
    schema: dataSchema,
    rows: data
  };
};

function getAuthType() {
  var response = {
    "type": "NONE"
  };
  return response;
}

function test() {
  var testDomain = "https://memfacts.data.socrata.com";
  var testID = "r84r-ys55";
  var testDateCol = "creation_date";
  var testDaysAgo = 14;
  var testMonthsAgo = 13;
  var testYearsAgo = 1;
  
  var daysAgo = moment().subtract(testDaysAgo, 'days');
  var monthsAgo = moment().subtract(testMonthsAgo, 'months');
  var yearsAgo = moment().subtract(testYearsAgo, 'years');
  var upto = moment().subtract(1, 'months');
  
  var user_fields = [{"name":"incident_id","label":"Employee Status","dataType":"STRING"},{"name":"division","label":"Incident ID","dataType":"STRING"}];
  var api_fields = ["incident_id", "division"];
  var api_queries = [testDateCol, monthsAgo.format("YYYY-MM-DD"), upto.format("YYYY-MM-DD")];
  
  var schema = schemaInit(testDomain, testID);
  var tableSchema = toTableSchema(schema);
  var dataDyn = dataDynamicInit(testDomain, testID, api_fields, api_queries);
  var mapped = dataDyn.map(function(row) { return toRowResponse(tableSchema, row); });
  var data = dataInit(testDomain, testID, api_fields);
  var mapped = data.map(function(row) { return toRowResponse(tableSchema, row); });
  Logger.log(mapped.length);
  
}
