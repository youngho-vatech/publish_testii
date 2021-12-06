const moment = require("moment");

const getType = object => {
  if (object.constructor === Object) return "Map";
  if (object.constructor === Array) return "List";
  if (object.constructor === Number) return "Number";
  if (object.constructor === String) return "String";
  if (object.constructor === Boolean) return "Boolean";
  return "Unknown";
};

const getRequiredKeys = (schema, sortKey = null) => {
  let globalHashKey = "";
  let originHashKey = "";
  let key = Object.keys(schema).filter(key => {
    const value = schema[key];

    if (sortKey && key === sortKey) return true;
    if (value.hasOwnProperty("globalIndex") && sortKey) {
      globalHashKey = value.globalIndex.hashKey;
      return false;
    }
    if (typeof value === "function") return false;
    if (value.hasOwnProperty("hashKey")) {
      originHashKey = key;
      return true;
    }
    if (!sortKey && value.hasOwnProperty("rangeKey")) return true;
    return false;
  });
  if (globalHashKey) {
    key.push(globalHashKey);
    if (key.includes(originHashKey))
      key = key.filter(el => {
        if (el !== originHashKey) return true;
        return false;
      });
  }
  return key;
};

const getHashRangeKeyIndex = schema => {
  const data = Object.keys(schema).reduce(
    (acc, cur) => {
      if (schema[cur].hashKey) acc.hashKey = cur;
      if (schema[cur].rangeKey) acc.rangeKey = cur;
      if (schema[cur].index) acc.indexs.push(cur);
      if (schema[cur].globalIndex)
        acc.globalIndexs.push({
          name: cur + "_global",
          hashKey: schema[cur].globalIndex.hashKey,
          hashKeyType: schema[cur].globalIndex["type"].name,
          indexKey: cur,
          indexKeyType: schema[cur]["type"].name
        });

      return acc;
    },
    {
      hashKey: null,
      rangeKey: null,
      indexs: [],
      globalIndexs: []
    }
  );

  return [data.hashKey, data.rangeKey, data.indexs, data.globalIndexs];
};

const getHashKey = schema => {
  const fIndex = Object.keys(schema).findIndex(key => {
    const value = schema[key];

    if (typeof value === "function") return false;
    if (value.hasOwnProperty("hashKey")) return true;

    return false;
  });

  if (fIndex !== -1) return Object.keys(schema)[fIndex];

  return null;
};
const getGlobalIndexHashKey = (schema, index) => {
  if (!index.split("_")[1]) return null;
  let gsiIndex = index.split("_")[0];

  let gsiHash = "";
  const gsiKey = Object.keys(schema).filter(key => {
    const value = schema[key];

    if (typeof value === "function") return false;
    if (value.hasOwnProperty("globalIndex") && key === gsiIndex) {
      gsiHash = value.globalIndex.hashKey;
      return true;
    }

    return false;
  });

  return gsiHash;
};
const getRangeKey = schema => {
  const fIndex = Object.keys(schema).findIndex(key => {
    const value = schema[key];

    if (typeof value === "function") return false;
    if (value.hasOwnProperty("rangeKey")) return true;

    return false;
  });

  if (fIndex !== -1) return Object.keys(schema)[fIndex];

  return null;
};

const envTestBool = value => (value === "true" ? true : false);

const getDateToTimestamp = value => moment.utc(value * 1000).unix();

const getUniqueKey = (schema, args) => {
  const data = Object.keys(schema).reduce(
    (acc, cur) => {
      if (schema[cur].hashKey) acc.hashKey = cur;
      if (schema[cur].rangeKey) acc.rangeKey = cur;
      if (schema[cur].index) acc.indexs.push(cur);

      return acc;
    },
    { hashKey: null, rangeKey: null, indexs: [] }
  );

  const keys = {};
  if (data.hashKey) keys[data.hashKey] = args[data.hashKey];
  if (data.rangeKey) keys[data.rangeKey] = args[data.rangeKey];
  return keys;
};

const splitForEach = (arr, n, tableName) => {
  let res = {};

  res[tableName] = arr[tableName].splice(0, n);
  return res;
};

module.exports = {
  getType,
  getRequiredKeys,
  getHashRangeKeyIndex,
  getHashKey,
  getRangeKey,
  envTestBool,
  getDateToTimestamp,
  getUniqueKey,
  splitForEach,
  getGlobalIndexHashKey
};
