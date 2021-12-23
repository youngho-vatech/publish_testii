const AWS = require("aws-sdk");
const dotenv = require("dotenv");
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

dotenv.config();

const debugTime = false;

const CREATE = "0";
const UPDATE = "1";
const DELETE = "2";

const dynamoDBCreateModels = options => {
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
  }

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
      acc[cur.toLowerCase()] = dynamoDBApi(schema[cur], cur, prefix || "");
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

const dynamoDBApi = (schema, tableName, prefix = "") => {
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

module.exports = {
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup,
  dynamoDBApi
};
