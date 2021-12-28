const { flatten } = require("flat");
const https = require("https");
const moment = require("moment");

const {
  getHashRangeKeyIndex,
  getRangeKey,
  getRequiredKeys,
  getType,
  getHashKey,
  splitForEach,
  getGlobalIndexHashKey
} = require("../utils/etc.js");

const getSchemaType = (schemas, name) => {
  const value = schemas[name];
  if (value.name) return value.name;
  if (value.type && value.type === "list") return "List";
  if (value.type) return value.type.name;
  if (value.constructor === Object) return "Map";
  if (value.constructor === Array) return "List";
  return "Unknown";
};

const checkExistsToDynamoDbName = tableName =>
  new Promise((resolve, reject) => {
    const dynamodb = new AWS.DynamoDB();
    function listTable(params = {}) {
      dynamodb
        .listTables(params)
        .promise()
        .then(data => {
          const exists =
            data.TableNames.filter(name => {
              return name === tableName;
            }).length > 0;
          if (exists) {
            return resolve(true);
          } else if (data.LastEvaluatedTableName) {
            const params = {
              ExclusiveStartTableName: data.LastEvaluatedTableName
            };
            return listTable(params);
          } else {
            return resolve(false);
          }
        })
        .catch(e => reject(e));
    }
    listTable();
  });

const createCommonFilter = (
  args,
  startKey,
  limit,
  comparison,
  match,
  obj,
  sort,
  isQuery = true,
  customFilter
) => {
  const { tableName, tableSchema, key, index } = obj;

  let hashKey = getHashKey(obj.tableSchema);

  let globalIndexHashKey = "";
  if (sort) {
    globalIndexHashKey = getGlobalIndexHashKey(obj.tableSchema, sort.indexKey);
  }

  const hashV = {};

  if (args) {
    if (globalIndexHashKey) {
      hashKey = globalIndexHashKey;

      hashV.Value = args[globalIndexHashKey];
      delete args[globalIndexHashKey];
    } else {
      hashV.Value = args[hashKey];
      delete args[hashKey];
    }
  }

  const hashValue = hashV.Value;

  const filter = {};
  filter.TableName =
    customFilter && customFilter.tableName ? customFilter.tableName : tableName;

  filter.ExpressionAttributeNames =
    customFilter && customFilter.ExpressionAttributeNames
      ? customFilter.ExpressionAttributeNames
      : {};

  filter.ExpressionAttributeValues =
    customFilter && customFilter.ExpressionAttributeValues
      ? customFilter.ExpressionAttributeValues
      : {};

  filter.FilterExpression = [];
  filter.KeyConditionExpression = [];

  if (hashValue) {
    filter.ExpressionAttributeNames[`#${hashKey}`] = `${hashKey}`;
    filter.ExpressionAttributeValues[`:${hashKey}`] = `${hashValue}`;
  }

  const keyList = getRequiredKeys(obj.tableSchema, sort ? sort.indexKey : "");
  if (args) {
    (args => {
      Object.keys(args).forEach(name => {
        if (name === "dummy") return;
        if (args[name] === null) return;

        const retType = getSchemaType(tableSchema, name);
        const argType = getType(JSON.parse(JSON.stringify(args[name])));
        const isKey = keyList.includes(name);
        const isIndex = index.includes(name);

        // console.log(`retType: ${retType}, argType: ${argType}`);
        if (retType === "String") {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;
          if (argType === "List") {
            args[name].forEach((el, index) => {
              filter.ExpressionAttributeValues[`:${name}${index}`] = `${el}`;
            });
          } else {
            filter.ExpressionAttributeValues[`:${name}`] = `${args[name]}`;
          }

          if (isKey && isQuery) {
            if (argType === "List") {
              const value = args[name].map((el, index) => {
                return `:${name}${index}`;
              });
              filter.KeyConditionExpression.push(`#${name} IN (${value})`);
            } else {
              filter.KeyConditionExpression.push(`#${name} = :${name}`);
            }
          } else {
            if (argType === "List") {
              const value = args[name].map((el, index) => {
                return `:${name}${index}`;
              });
              filter.FilterExpression.push(`#${name} IN (${value})`);
            } else {
              filter.FilterExpression.push(
                match === "contains"
                  ? `contains(#${name}, :${name})`
                  : `#${name} = :${name}`
              );
            }
          }
        }
        if (retType === "Number") {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;
          filter.ExpressionAttributeValues[`:${name}`] = args[name];

          if (isKey && isQuery) {
            filter.KeyConditionExpression.push(`#${name} = :${name}`);
          } else {
            filter.FilterExpression.push(`#${name} = :${name}`);
          }
        }
        if (retType === "Date") {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;
          filter.ExpressionAttributeValues[`:${name}Begin`] = args[name].begin;
          filter.ExpressionAttributeValues[`:${name}End`] = args[name].end;

          if (
            (isKey && isQuery) ||
            (isIndex && sort && sort.indexKey === name)
          ) {
            filter.KeyConditionExpression.push(
              `#${name} between :${name}Begin and :${name}End`
            );
          } else {
            filter.FilterExpression.push(
              `#${name} between :${name}Begin and :${name}End`
            );
          }
        }
        if (retType === "Boolean") {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;
          filter.ExpressionAttributeValues[`:${name}`] = args[name];
          filter.FilterExpression.push(`#${name} = :${name}`);
        }
        if (retType === "Map") {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;
          const flatMap = flatten(args[name]);
          Object.keys(flatMap).forEach(key => {
            const value = flatMap[key];
            key.split(".").forEach(el => {
              filter.ExpressionAttributeNames[`#${el}`] = `${el}`;
            });

            const newKey =
              `#${name}.` +
              key
                .split(`.`)
                .map(name => `#${name}`)
                .join(`.`);

            const newValueKey = key.split(`.`).join("");

            filter.ExpressionAttributeValues[`:${newValueKey}`] =
              typeof value === "string" ? `${value}` : value;

            if (typeof value === "string" && match === "contains") {
              filter.FilterExpression.push(
                `contains(${newKey}, :${newValueKey})`
              );
            } else {
              filter.FilterExpression.push(`${newKey} = :${newValueKey}`);
            }
          });
        }

        if (retType === "List" && ["String", "Number"].includes(argType)) {
          filter.ExpressionAttributeNames[`#${name}`] = `${name}`;

          if (args[name][0] === "!") {
            filter.ExpressionAttributeValues[`:${name}`] = `${args[
              name
            ].substring(1)}`;
            filter.FilterExpression.push(`not contains(#${name}, :${name})`);
          } else {
            filter.ExpressionAttributeValues[`:${name}`] = args[name];
            filter.FilterExpression.push(`contains(#${name}, :${name})`);
          }
        }
      });
    })(JSON.parse(JSON.stringify(args)));
  }

  filter.FilterExpression = filter.FilterExpression.join(` ${comparison} `);
  if (filter.KeyConditionExpression.length)
    filter.KeyConditionExpression = filter.KeyConditionExpression.join(
      ` ${comparison} `
    );
  else delete filter.KeyConditionExpression;

  if (hashValue) {
    if (isQuery) {
      const tmp = filter.KeyConditionExpression
        ? ` and ${filter.KeyConditionExpression}`
        : "";
      filter.KeyConditionExpression = `#${hashKey} = :${hashKey}${tmp}`;
    } else {
      const tmp = filter.FilterExpression
        ? ` and ${filter.FilterExpression}`
        : "";
      filter.FilterExpression = `#${hashKey} = :${hashKey}${tmp}`;
    }
  }

  if (customFilter) {
    if (
      customFilter.KeyConditionExpression &&
      customFilter.KeyConditionExpression.length
    ) {
      if (filter.KeyConditionExpression) {
        const tmp = filter.KeyConditionExpression
          ? ` and (${filter.KeyConditionExpression})`
          : "";
        filter.KeyConditionExpression = `${customFilter.KeyConditionExpression.join(
          ` and `
        )}${tmp}`;
      } else {
        filter.KeyConditionExpression = customFilter.KeyConditionExpression[0];
      }
    }

    if (customFilter.FilterExpression && customFilter.FilterExpression.length) {
      const comparison = customFilter.Comparison
        ? customFilter.Comparison
        : `and`;
      const tmp = filter.FilterExpression
        ? ` ${comparison} (${filter.FilterExpression})`
        : "";
      filter.FilterExpression = `${customFilter.FilterExpression.join(
        ` ${comparison} `
      )}${tmp}`;
    }

    if (customFilter.ProjectionExpression) {
      const arr = [];
      if (!filter.ExpressionAttributeNames)
        filter.ExpressionAttributeNames = {};

      customFilter.ProjectionExpression.forEach(el => {
        const item = `#${el}`;
        if (!filter.ExpressionAttributeNames[item])
          filter.ExpressionAttributeNames[item] = el;

        arr.push(item);
      });

      filter.ProjectionExpression = arr.join(",");
    }
  }

  if (!filter.FilterExpression) {
    if (!filter.KeyConditionExpression) {
      delete filter.ExpressionAttributeNames;
      delete filter.ExpressionAttributeValues;
    }

    if (isQuery) delete filter.FilterExpression;
    else filter.FilterExpression = "attribute_not_exists(dummy)";
  }

  if (startKey) filter.ExclusiveStartKey = startKey;
  if (limit) filter.Limit = limit;

  if (sort) {
    filter.IndexName = sort.indexKey;
    filter.ScanIndexForward = !Boolean(sort.order);
  }

  return filter;
};

module.exports = { createCommonFilter };
