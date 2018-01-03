var SpreadsheetApp = SpreadsheetApp || undefined;

/**
 * Custom cache backed by Google Sheets.
 *
 * @param {string} organization - The organization name.
 * @param {string} repository - The repository name.
 * @return {object} the cache object.
 */
function CustomCache(organization, repository) {
  var spreadsheet =
      CustomCache.SpreadsheetApp.openById(CustomCache.SPREADSHEET_ID);
  this.spreadsheet = spreadsheet;
  var sheetName = CustomCache.buildSheetName(organization, repository);
  var sheet = spreadsheet.getSheetByName(sheetName);
  if (sheet === null) {
    spreadsheet.insertSheet(sheetName);
  }
  this.sheet = spreadsheet.getSheetByName(sheetName);
  return this;
}

CustomCache.SPREADSHEET_ID_PROPERTY = 'SPREADSHEET_ID';

CustomCache.SPREADSHEET_ID =
    PropertiesService.getScriptProperties().getProperty(
        CustomCache.SPREADSHEET_ID_PROPERTY);

CustomCache.SpreadsheetApp = SpreadsheetApp;

/**
 * Builds and returns a formatted string to be used for the sheet name.
 *
 * @param {string} organization - The organization name.
 * @param {string} repository - The repository name.
 * @return {string} The built sheet name.
 */
CustomCache.buildSheetName = function(organization, repository) {
  return organization + ' :: ' + repository;
};

/**
 * Clears the cache.
 */
CustomCache.prototype.clear = function() {
  var range = this.getEntireRange();
  range.clear();
};

/**
 * Puts the key and value into the cache.
 *
 * @param {string|number} key - the key to use. This key will be stringified.
 * @param {object} value - the value to use. This value will be stringified.
 */
CustomCache.prototype.put = function(key, value) {
  var stringifiedKey = this.stringify(key);
  var stringifiedValue = this.stringify(value);
  var row = [stringifiedKey, stringifiedValue];
  this.sheet.appendRow(row);
};

/**
 * Stringifies objects and primatives. This ensures that our keys and values
 * always end up as the same strings.
 *
 * @param {any} value - The value to stringify.
 * @return {string} A string representation of value.
 */
CustomCache.prototype.stringify = function(value) {
  if (typeof value === 'object') {
    return JSON.stringify(value);
  } else {
    return '' + value;
  }
};

/**
 * Returns the range for the sheet.
 *
 * @return {object} the range for the sheet.
 */
CustomCache.prototype.getEntireRange = function() {
  var lastRow = this.sheet.getLastRow();
  lastRow = (lastRow < 1) ? 1 : lastRow;
  return this.sheet.getRange(1, 1, lastRow, 2);
};

/**
 * Gets the value for the key, or undefined if the key isn't present in the
 * cache.
 *
 * @param {string|number} key - The key to lookup in the cache.
 * @return {object|undefined} The value stored in the cache.
 */
CustomCache.prototype.get = function(key) {
  var range = this.getEntireRange();
  var stringyKey = this.stringify(key);
  for (var i = 1; i <= range.getLastRow(); i++) {
    var rowKey = range.getCell(i, 1).getValue();
    if (rowKey === stringyKey) {
      return JSON.parse(range.getCell(i, 2).getValue());
    }
  }
  return undefined;
};

/**
 * Finds the row that contains the data for the key, and returns that plus all
 * subsequent rows.
 *
 * @param {string|number} key - The key to lookup in the cache.
 * @return {object|undefined} The value stored in the cache.
 */
CustomCache.prototype.getPaginated = function(key) {
  var range = this.getEntireRange();
  var stringyKey = this.stringify(key);
  for (var i = 1; i <= range.getLastRow(); i++) {
    var rowKey = range.getCell(i, 1).getValue();
    if (rowKey === stringyKey) {
      var j = i;
      var results = [];
      var nextLink;
      while (j <= range.getLastRow()) {
        // get the rest of the rows and join them together.
        var cachedValue = JSON.parse(range.getCell(j, 2).getValue());
        results = results.concat(cachedValue.json);
        nextLink = cachedValue.nextLink;
        j++;
      }
      return {json: results, nextLink: nextLink};
    }
  }
  return undefined;
};

// Needed for testing
var module = module || {};
module.exports = CustomCache;
