import AWS from "aws-sdk";
import * as dotenv from "dotenv";
// import dynamoose from "dynamoose";
import { flatten } from "flat";
import https from "https";
import moment from "moment";

import {
  getHashRangeKeyIndex,
  getRangeKey,
  getRequiredKeys,
  getType,
  getHashKey,
  splitForEach,
  getGlobalIndexHashKey
} from "../../utils";

dotenv.config();

const debugTime = false;

const CREATE = "0";
const UPDATE = "1";
const DELETE = "2";
const BATCH_PUT = "3";
const PRINT = "4";
const LOGIN = "10";
const LOGOUT = "11";
const CREATE_PERM = "20";
const UPDATE_PERM = "21";
const DELETE_PERM = "22";

const createModels = options => {
  const { profile, prefix, importSchema = true, schema } = options;
  console.log(
    `profile: ${profile}`,
    `prefix: ${prefix}, importSchema: ${importSchema}`
  );
  const credentials = new AWS.SharedIniFileCredentials({
    profile: profile || "default"
  });

  if (credentials.accessKeyId) {
    AWS.config.credentials = credentials;
    // dynamoose.AWS.config.credentials = credentials;
  }

  // dynamoose.AWS.config.update({
  //   region: process.env.AWS_DEFAULT_REGION || "ap-northeast-2",
  //   httpOptions: {
  //     agent: new https.Agent({
  //       rejectUnauthorized: true,
  //       keepAlive: true
  //     })
  //   }
  // });

  AWS.config.update({
    region: process.env.AWS_DEFAULT_REGION || "ap-northeast-2",
    httpOptions: {
      agent: new https.Agent({
        rejectUnauthorized: true,
        keepAlive: true
      })
    }
  });

  let models = {};
  if (importSchema) {
    models = Object.keys(schema).reduce((acc, cur) => {
      // console.log(schema[cur], cur, prefix || "");
      acc[cur.toLowerCase()] = api(schema[cur], cur, prefix || "");
      return acc;
    }, {});
  }

  models.dynamodb = new AWS.DynamoDB();

  models.listTable = async options => {
    const { startKey: ExclusiveStartTableName } = options;

    return new Promise((resolve, reject) => {
      models.dynamodb.listTables({ ExclusiveStartTableName }, (err, data) => {
        resolve({
          err,
          data: data ? data.TableNames : null,
          lastKey: data ? data.LastEvaluatedTableName : null
        });
      });
    });
  };

  models.deleteTable = async options => {
    const { name: TableName } = options;

    // return;
    return new Promise((resolve, reject) => {
      models.dynamodb.deleteTable({ TableName }, (err, data) => {
        resolve({
          err,
          data
        });
      });
    });
  };

  return models;
};

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

const checkStatusToDynamoDbName = async tableName => {
  const exists = await checkExistsToDynamoDbName(tableName);
  return new Promise((resolve, reject) => {
    if (exists) {
      const dynamodb = new AWS.DynamoDB();
      const params = {
        TableName: tableName
      };
      dynamodb.describeTable(params, (err, data) => {
        if (err) reject(err);
        resolve(data.Table.TableStatus);
        // "ACTIVE", "CREATING"
      });
    } else {
      resolve("NOT_FOUND");
    }
  });
};

const getSecretString = SecretId => {
  return new Promise((resolve, reject) => {
    const sm = new AWS.SecretsManager();
    sm.getSecretValue({ SecretId }, function (err, data) {
      if (err) reject(err);
      else resolve(data.SecretString);
    });
  });
};

const restoreTableFromBackup = (tableName, backupArn) => {
  return new Promise((resolve, reject) => {
    const dynamodb = new AWS.DynamoDB();
    const params = {
      BackupArn: backupArn,
      TargetTableName: tableName
    };
    dynamodb.restoreTableFromBackup(params, (err, data) => {
      if (err) reject(err);
      resolve(data);
    });
  });
};

// const getTreatTableName = (tableName, prefix, hashValue) => {
//   if (!(tableName.split("-")[1] === "Treat") || !hashValue) {
//     return tableName;
//   }

//   return `${prefix}${hashValue}-Treat`;
// };

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

const createQueryFilter = (
  args,
  startKey,
  limit,
  comparison,
  match,
  obj,
  sort,
  customFilter
) => {
  const filter = createCommonFilter(
    args,
    startKey,
    limit,
    comparison,
    match,
    obj,
    sort,
    true,
    customFilter
  );

  return filter;
};

const createScanFilter = (
  args,
  startKey,
  limit,
  comparison,
  match,
  segment,
  totalSegments,
  obj,
  sort,
  tmp
) => {
  var filter = createCommonFilter(
    args,
    startKey,
    limit,
    comparison,
    match,
    obj,
    sort,
    false,
    tmp
  );
  filter.Segment = segment;
  filter.TotalSegments = totalSegments;

  return filter;
};

const createGetFilter = (args, projectionFilter, obj) => {
  const filter = {
    TableName: obj.tableName,
    Key: args
  };

  if (projectionFilter) {
    const arr = [];
    filter.ExpressionAttributeNames = {};

    projectionFilter.forEach(el => {
      const item = `#${el}`;
      filter.ExpressionAttributeNames[item] = el;
      arr.push(item);
    });

    filter.ProjectionExpression = arr.join(",");
  }

  return filter;
};

const createBatchGetFilter = (args, obj) => {
  let keys = [];
  for (var i = 0; i < args.length; i++) keys.push(args[i]);
  const filter = { RequestItems: { [obj.tableName]: { Keys: keys } } };

  return filter;
};

const createBatchPutFilter = (args, obj) => {
  let params = { [obj.tableName]: [] };

  args.forEach(arg => {
    params[obj.tableName].push({
      PutRequest: { Item: arg }
    });
  });

  return params;
};

const isEmpty = function (value) {
  if (
    value == null ||
    value == undefined ||
    (value != null && typeof value == "object" && !Object.keys(value).length)
  ) {
    if (typeof value == "boolean" || Array.isArray(value)) {
      return false;
    }
    return true;
  } else {
    return false;
  }
};

// 새로 만든 filter로 업데이트에 관한 filter를 제작함
const createUpdateFilter = (_args, obj) => {
  //FIXME:
  var args = JSON.parse(JSON.stringify(_args));
  for (var key in args) {
    if (isEmpty(args[key])) delete args[key];
  }
  const filter = {};

  var updated = [];
  const hashKey = getHashKey(obj.tableSchema);
  const rangeKey = getRangeKey(obj.tableSchema);
  const indexKey = getHashRangeKeyIndex(obj.tableSchema)[2];

  let count = 0;
  let value = [];
  // hashkey만 존재하는 경우가 있으므로 rangekey와 구분할 예외처리를 진행함
  for (var key in args) {
    if (key == hashKey) {
      value[0] = args[key];
      count++;
    } else if (key == rangeKey) {
      value[1] = args[key];
      count++;
    }
    if (count == 2) break;
  }
  const hashValue = value[0];
  const rangeValue = value[1];

  count = 0;
  for (var key in args) {
    if (key === hashKey) continue;
    else if (key === rangeKey) continue;
    else if (args[key] === undefined) continue;
    else if (indexKey.includes(key) && args[key] == "") continue; // 빈값의 업데이트를 허용하되 key로 지정한 값은 빈 값이 허용되지 않는다.
    count += 1;
    updated.push(key);
  }

  filter.TableName = `${obj.tableName}`;

  filter.Key = {};
  if (hashKey) filter.Key[hashKey] = hashValue;
  if (rangeKey) filter.Key[rangeKey] = rangeValue;

  if (updated.length > 0) {
    filter.UpdateExpression = "";
    filter.ExpressionAttributeNames = {};
    filter.ExpressionAttributeValues = {};

    filter.UpdateExpression = `set `;

    for (let index = 0; index < count; index++) {
      filter.UpdateExpression += `#${updated[index]}= :${updated[index]}`;
      if (index != count - 1) {
        filter.UpdateExpression += `, `;
      }
      filter.ExpressionAttributeNames[
        `#${updated[index]}`
      ] = `${updated[index]}`;
      filter.ExpressionAttributeValues[`:${updated[index]}`] =
        args[updated[index]];
    }
  }

  return filter;
};

const createBatchWriteParams = (args, obj) => {
  let params = { [obj.tableName]: [] };
  args.forEach(arg => {
    params[obj.tableName].push({
      DeleteRequest: {
        Key: arg
      }
    });
  });

  return params;
};

const get = async filter => {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.get(filter, function (err, data) {
      if (err) {
        return reject(err);
      } else {
        return resolve(data.Item);
      }
    });
  });
};

const batchGet = async filter => {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.batchGet(filter, function (err, data) {
      if (err) {
        return reject(err);
      } else {
        return resolve(data.Responses);
      }
    });
  });
};

const batchWrite = async params => {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.batchWrite(params, function (err, data) {
      if (err) {
        return reject(err);
      } else {
        return resolve(data.UnprocessedItems);
      }
    });
  });
};

const query = async filter => {
  return new Promise((resolve, reject) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    docClient.query(filter, function (err, data) {
      if (err) {
        return reject(err);
      } else {
        return resolve(data);
      }
    });
  });
};

// const scan = async filter => {
//   return new Promise((resolve, reject) => {
//     const docClient = new AWS.DynamoDB.DocumentClient();
//     docClient.scan(filter, function (err, data) {
//       if (err) {
//         return reject(err);
//       } else {
//         return resolve({ items: data.Items, lastKey: data.LastEvaluatedKey });
//       }
//     });
//   });
// };

// const model = (tableName, schema, prefix) => {
//   return dynamoose.model(
//     tableName,
//     new dynamoose.Schema(schema, { throughput: "ON_DEMAND" }),
//     {
//       update: true,
//       prefix
//     }
//   );
// };

// const getTable = (prefix, tableName, schema, hospitalId, table) => {
//   if (tableName === "Treat" && hospitalId) {
//     return model(tableName, schema, prefix + hospitalId + "-");
//   }

//   return table;
// };

const api = (schema, tableName, prefix = "") => {
  const obj = new Object();
  const rangeKey = getRangeKey(schema);
  const requiredKeys = getRequiredKeys(schema);

  // const table = model(tableName, schema, prefix);

  obj.tableName = `${prefix}${tableName}`;
  obj.tableSchema = schema;
  obj.prefix = prefix;

  obj.getRequiredKeys = sortKey => {
    return getRequiredKeys(schema, sortKey);
  };

  obj.index = Object.keys(schema).filter(key => {
    const value = schema[key];
    if (typeof value === "function") return false;

    if (value.hasOwnProperty("index")) {
      return true;
    }

    return false;
  });

  obj.insert = async (args, context) => {
    const docClient = new AWS.DynamoDB.DocumentClient();
    let newArgs = {};
    if (context && context.pre) {
      newArgs = await context.pre(args, context, false);
    } else {
      newArgs = args;
    }
    const params = createUpdateFilter(newArgs, obj);
    params.ReturnValues = "ALL_NEW";

    return await docClient
      .update(params)
      .promise()
      .then(async result => {
        if (context && context.post) {
          await context.post(
            tableName,
            CREATE,
            newArgs,
            rangeKey,
            result.Attributes,
            context
          );
        }

        return result.Attributes;
      })
      .catch(e => {
        console.log(e);
      });
  };

  obj.batchPut = async (args, context, isUpdate = false) => {
    const mapArgs = args.map(el => {
      if (context && context.pre) {
        el = context.pre(el, context, isUpdate);
      }
      return el;
    });

    const newArgs = await Promise.all(mapArgs);

    const __batchWrite = async items => {
      const BATCH_WRITE_MAXIMUM_UNITS = 25;
      if (items[obj.tableName].length === 0) return;
      let newItems = splitForEach(
        items,
        BATCH_WRITE_MAXIMUM_UNITS,
        obj.tableName
      );

      return await batchWrite({ RequestItems: newItems })
        .then(async result => {
          if (context && context.post) {
            for (let i = 0; i < newArgs.length; i++) {
              await context.post(
                tableName,
                isUpdate ? UPDATE : CREATE,
                newArgs[i],
                rangeKey,
                newArgs[i],
                context
              );
            }
          }

          return await __batchWrite(items);
        })
        .catch(e => {
          console.log(e);
        });
    };

    const params = createBatchPutFilter(newArgs, obj);

    if (!params) return;

    await __batchWrite(params);
  };

  obj.update = async (args, context) => {
    let newArgs = {};
    if (context && context.pre) {
      newArgs = await context.pre(args, context, true);
    } else {
      newArgs = args;
    }
    const docClient = new AWS.DynamoDB.DocumentClient();
    const params = createUpdateFilter(newArgs, obj);
    params.ReturnValues = "ALL_NEW";

    const res = await docClient
      .update(params)
      .promise()
      .then(async result => {
        if (context && context.post) {
          await context.post(
            tableName,
            UPDATE,
            newArgs,
            rangeKey,
            result.Attributes,
            context
          );
        }

        return result;
      })
      .catch(e => {
        console.log(e);
      });
    return res.Attributes;
  };

  obj.remove = async (args, context) => {
    let value = [];
    let count = 0;
    const hashKey = getHashKey(obj.tableSchema);
    const rangeKey = getRangeKey(obj.tableSchema);
    for (var key in obj.tableSchema) {
      if (key == hashKey) {
        value[0] = args[key];
        count++;
      } else if (key == rangeKey) {
        value[1] = args[key];
        count++;
      }
      if (count == 2) break;
    }
    const hashValue = value[0];
    const rangeValue = value[1];
    const instance = {};
    instance[`${hashKey}`] = hashValue;
    instance[`${rangeKey}`] = rangeValue;

    const params = {
      TableName: obj.tableName,
      Key: instance
    };
    params.ReturnValues = "ALL_OLD";
    const docClient = new AWS.DynamoDB.DocumentClient();
    return await docClient
      .delete(params)
      .promise()
      .then(async result => {
        if (context && context.post) {
          await context.post(
            tableName,
            DELETE,
            args,
            rangeKey,
            result.Attributes,
            context
          );
        }

        return result.Attributes;
      })
      .catch(e => {
        console.log(e);
      });
  };

  obj.get = async (args, projectionFilter) => {
    for (var key in args) if (!args[key]) return;

    const filter = createGetFilter(args, projectionFilter, obj);

    return await get(filter)
      .then(result => {
        return result;
      })
      .catch(e => {
        console.log(e);
      });
  };

  obj.batchGet = async args => {
    for (var key in args) if (!args[key]) return;

    const filter = createBatchGetFilter(args, obj);
    return await batchGet(filter)
      .then(result => {
        return result[obj.tableName];
      })
      .catch(e => {
        console.log(e);
      });
  };

  obj.batchWrite = async args => {
    const __batchWrite = async items => {
      return await batchWrite({ RequestItems: items })
        .then(async result => {
          if (Object.keys(result).length) {
            return await __batchWrite(result);
          }

          return result;
        })
        .catch(e => {
          console.log(e);
        });
    };

    const params = createBatchWriteParams(args, obj);

    if (!params) return;

    await __batchWrite(params);
  };

  obj.increment = (args, target, value = 1) => {
    return new Promise((resolve, reject) => {
      const docClient = new AWS.DynamoDB.DocumentClient();
      const params = {
        TableName: obj.tableName,
        Key: args,
        UpdateExpression: "set #t = #t + :v",
        ExpressionAttributeNames: { "#t": target },
        ExpressionAttributeValues: { ":v": value },
        ReturnValues: "UPDATED_NEW"
      };

      docClient.update(params, function (err, data) {
        if (err) {
          console.log(err);
          return reject(false);
        } else {
          return resolve(data.Attributes[target]);
        }
      });
    });
  };

  obj.decrement = (args, target, value = 1) => {
    return new Promise((resolve, reject) => {
      const docClient = new AWS.DynamoDB.DocumentClient();
      const params = {
        TableName: obj.tableName,
        Key: args,
        UpdateExpression: "set #t = #t - :v",
        ExpressionAttributeNames: { "#t": target },
        ExpressionAttributeValues: { ":v": value },
        ReturnValues: "UPDATED_NEW"
      };

      docClient.update(params, function (err, data) {
        if (err) {
          console.log(err);
          return reject(false);
        } else {
          return resolve(data.Attributes[target]);
        }
      });
    });
  };

  obj.incrementMap = (args, target, type, value = 1) => {
    return new Promise((resolve, reject) => {
      const hashKey = getHashKey(obj.tableSchema);
      const hashValue = args[hashKey];

      const keyFilter = {};
      keyFilter[hashKey] = hashValue;
      const docClient = new AWS.DynamoDB.DocumentClient();
      const params = {
        TableName: obj.tableName,
        Key: keyFilter,
        UpdateExpression: "set #c.#t = #c.#t + :v",
        ExpressionAttributeNames: {
          "#c": target,
          "#t": type
        },
        ExpressionAttributeValues: { ":v": value },
        ReturnValues: "UPDATED_NEW"
      };

      docClient.update(params, function (err, data) {
        if (err) {
          console.log(err);
          return reject(false);
        } else {
          return resolve(data.Attributes[target][type]);
        }
      });
    });
  };

  // comparison: [and || or]
  // match: [eq || contains]
  obj.searchAll = async (
    args,
    comparison = "and",
    match = "eq",
    tCnt = 1,
    sort,
    customFilter
  ) => {
    if (debugTime) var date1 = moment();
    const docClient = new AWS.DynamoDB.DocumentClient();
    const __searchAll = async (startKey, limit, comparison, match, filter) => {
      if (startKey) filter.ExclusiveStartKey = startKey;

      return await docClient
        .scan(filter)
        .promise()
        .then(async items => {
          if (items.LastEvaluatedKey) {
            const addItems = await __searchAll(
              items.LastEvaluatedKey,
              limit,
              comparison,
              match,
              filter
            );
            items.Items = items.Items.concat(addItems);
            items.LastEvaluatedKey = addItems.LastEvaluatedKey;
            return items.Items;
          }

          return items.Items;
        })
        .catch(e => {
          console.log(e);
        });
    };

    const retList = [];
    for (var i = 0; i < tCnt; i++) {
      const filter = createScanFilter(
        args,
        null,
        0,
        comparison,
        match,
        i,
        tCnt,
        obj,
        sort,
        customFilter
      );

      retList.push(__searchAll(null, 0, comparison, match, filter));
    }

    const data = await Promise.all(retList).then(retList => {
      let ret = [];
      retList.forEach(item => {
        ret = ret.concat(item);
      });

      return ret;
    });

    if (debugTime) {
      const date2 = moment();
    }

    return data;
  };

  obj.searchPagination = async (
    args,
    startKey = null,
    limit = 100,
    comparison = "and",
    match = "eq",
    tCnt = 1,
    sort,
    customFilter
  ) => {
    const buffer = new SharedArrayBuffer(16);
    const uint8 = new Uint8Array(buffer);
    uint8[0] = 0;
    const docClient = new AWS.DynamoDB.DocumentClient();
    if (debugTime) var date1 = moment();

    const __searchLimit = async (startKey, limit, filter) => {
      if (startKey) filter.ExclusiveStartKey = startKey;
      return await docClient
        .scan(filter)
        .promise()
        .then(async items => {
          const tot = Atomics.add(uint8, 0, items.Count);
          if (items.LastEvaluatedKey && tot + items.Count < limit) {
            const addItems = await __searchLimit(
              items.LastEvaluatedKey,
              limit,
              filter
            );
            items.Items = items.Items.concat(addItems.Items);
            items.LastEvaluatedKey = addItems.LastEvaluatedKey;

            return items;
          }

          return items;
        })
        .catch(e => {
          console.log(e);
        });
    };

    const retList = [];

    for (var i = 0; i < tCnt; i++) {
      if (startKey && !startKey[i]) {
        const tmp = [];
        tmp.LastEvaluatedKey = null;
        retList.push(tmp);
        continue;
      }

      const sKey = startKey ? startKey[i] : null;
      const filter = createScanFilter(
        args,
        sKey,
        limit,
        comparison,
        match,
        i,
        tCnt,
        obj,
        sort,
        customFilter
      );

      retList.push(__searchLimit(sKey, limit, filter));
    }

    const [result, lastKey] = await Promise.all(retList).then(retList => {
      let retArr = [];

      let lastKeyArr = [];
      let tot = 0;
      let isLimit = false;
      retList.forEach((el, index) => {
        let item = el;
        if (isLimit) {
          const lastKey = (() => {
            if (startKey === null) {
              if (!item.Items[0]) return item.LastEvaluatedKey;

              const keys = {};
              requiredKeys.forEach(el => (keys[el] = item.Items[0][el]));
              return keys;
            }
            if (startKey && !startKey[i]) {
              return null;
            }
            const keys = {};
            requiredKeys.forEach(el => (keys[el] = item.Items[0][el]));
            if (sort) keys[sort.indexKey] = item.Items[0][sort.indexKey];

            return keys;
          })();

          lastKeyArr = lastKeyArr.concat(lastKey);
          return;
        }

        if (tot + item.Items.length > limit) {
          const cnt = limit - tot;

          item.Items = item.Items.slice(0, cnt);

          if (item.Items.length > 0) {
            const keys = {};
            requiredKeys.forEach(el => (keys[el] = item.Items[cnt - 1][el]));
            if (sort)
              keys[sort.indexKey] = item.Items[limit - 1][sort.indexKey];

            item.LastEvaluatedKey = keys;
          } else {
            item.LastEvaluatedKey = null;
          }

          isLimit = true;
        }

        tot += item.Items.length;

        retArr = retArr.concat(item.Items);
        lastKeyArr = lastKeyArr.concat(item.LastEvaluatedKey);
      });

      return [retArr, lastKeyArr];
    });

    if (debugTime) {
      const date2 = moment();
    }

    result.lastKey = lastKey;

    return result;
  };

  obj.queryPagination = async (
    args,
    startKey = null,
    limit = 100,
    comparison = "and",
    match = "eq",
    sort,
    customFilter
  ) => {
    if (debugTime) var date1 = moment();

    const __queryLimit = async (startKey, limit, filter, cnt) => {
      if (startKey) filter.ExclusiveStartKey = startKey;

      return await query(filter)
        .then(async data => {
          var newCnt = cnt + data.Count;

          if (data.LastEvaluatedKey && newCnt < limit) {
            const [items, lastKey] = await __queryLimit(
              data.LastEvaluatedKey,
              limit,
              filter,
              newCnt
            );

            const newItems = data.Items.concat(items);
            return [newItems, lastKey];
          }

          return [data.Items, data.LastEvaluatedKey];
        })
        .catch(e => {
          console.log(e);
        });
    };

    const filter = createQueryFilter(
      args,
      startKey,
      100,
      comparison,
      match,
      obj,
      sort,
      customFilter
    );

    const [data, lastKey] = await __queryLimit(startKey, limit, filter, 0);

    if (debugTime) {
      var date2 = moment();
    }

    if (limit && data.length > limit) {
      const newData = data.slice(0, limit);
      const newLastKey = {};

      requiredKeys.forEach(key => {
        newLastKey[key] = newData[limit - 1][key];
      });

      if (sort) newLastKey[sort.indexKey] = newData[limit - 1][sort.indexKey];

      newData.lastKey = newLastKey;
      return newData;
    }

    data.lastKey = lastKey;

    return data;
  };

  obj.queryAll = async (
    args,
    comparison = "and",
    match = "eq",
    sort,
    customFilter
  ) => {
    if (debugTime) var date1 = moment();

    const __queryAll = async (startKey, limit, comparison, match, filter) => {
      if (startKey) filter.ExclusiveStartKey = startKey;
      return await query(filter).then(async data => {
        if (data.LastEvaluatedKey) {
          data.Items = data.Items.concat(
            await __queryAll(
              data.LastEvaluatedKey,
              limit,
              comparison,
              match,
              filter
            )
          );
          return data.Items;
        }
        return data.Items;
      });
    };

    const filter = createQueryFilter(
      args,
      null,
      0,
      comparison,
      match,
      obj,
      sort,
      customFilter
    );

    const ret = await __queryAll(null, 0, comparison, match, filter);

    if (debugTime) {
      const date2 = moment();
    }

    return ret;
  };

  obj.totalCnt = async (
    args,
    comparison = "and",
    match = "eq",
    sort,
    customFilter
  ) => {
    let total = 0;
    const __queryAll = async (startKey, limit, comparison, match, filter) => {
      if (startKey) filter.ExclusiveStartKey = startKey;
      return await query(filter).then(async data => {
        if (data.LastEvaluatedKey) {
          await __queryAll(
            data.LastEvaluatedKey,
            limit,
            comparison,
            match,
            filter
          );
        }
        total += data.Items.length;
      });
    };

    const filter = createQueryFilter(
      args,
      null,
      0,
      comparison,
      match,
      obj,
      sort,
      customFilter
    );

    await __queryAll(null, 0, comparison, match, filter);

    return total;
  };

  return obj;
};

export {
  createModels,
  getSecretString,
  restoreTableFromBackup,
  checkStatusToDynamoDbName,
  // model,
  api
};
