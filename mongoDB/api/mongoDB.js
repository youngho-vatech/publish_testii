import { flatten } from "flat";
import moment from "moment";
import mongoose from "mongoose";
import {
  envTestBool,
  getHashKey,
  getRequiredKeys,
  getHashRangeKeyIndex,
  getType,
  getGlobalIndexHashKey
} from "../../utils";

const mongoosePaginate = require("mongoose-paginate-v2");
const { Schema } = mongoose;

const CREATE = "0";
const UPDATE = "1";
const DELETE = "2";
const BATCH_PUT = "3";
const LOGIN = "10";
const LOGOUT = "11";
const CREATE_PERM = "20";
const UPDATE_PERM = "21";
const DELETE_PERM = "22";

const createModels = async options => {
  const { prefix, importSchema = true, schema } = options;

  var mongoDB =
    "mongodb://" +
    process.env.MONGODB_USERNAME +
    ":" +
    process.env.MONGODB_PASSWORD +
    "@" +
    process.env.MONGODB_HOSTNAME +
    ":" +
    process.env.MONGODB_PORT +
    "/" +
    process.env.MONGODB_DATABASE;

  console.log("dbconnection:", mongoDB);

  await mongoose
    .connect(mongoDB, {
      useNewUrlParser: true,
      useCreateIndex: true,
      useFindAndModify: false,
      useUnifiedTopology: envTestBool(process.env.MONGODB_UNIFIEDTOPOLOGY)
    })
    .then(res => {
      console.log("successfully connected to the database");
      return res;
    })
    .catch(e => {
      console.log("error connecting to the database: ", e);
    });

  let models = {};
  if (importSchema) {
    models = Object.keys(schema).reduce((acc, cur) => {
      acc[cur.toLowerCase()] = api(schema[cur], cur, prefix || "");
      return acc;
    }, {});
  }

  return models;
};

const getSchemaType = (schemas, name) => {
  const value = schemas.path(name);

  if (schemas.pathType(name) === "nested") {
    return "Map";
  }
  if (value instanceof Schema.Types.String) return "String";
  if (value instanceof Schema.Types.Number) return "Number";
  if (value instanceof Schema.Types.Date) return "Date";
  if (value instanceof Schema.Types.Boolean) return "Boolean";
  if (value instanceof Schema.Types.Array) return "List";
  return "Unknown";
};

const createCommonFilter = (
  args = {},
  startKey,
  limit,
  comparison,
  match,
  obj,
  sort,
  isQuery = true,
  customFilter
) => {
  const conditions = {};
  const { tableName, tableSchema, key, index } = obj;

  let hashKey = getHashKey(tableSchema.obj);
  let globalIndexHashKey = "";
  if (sort) {
    globalIndexHashKey = getGlobalIndexHashKey(tableSchema.obj, sort.indexKey);
  }
  const hashV = {};
  if (args) {
    if (globalIndexHashKey) {
      hashKey = globalIndexHashKey;
      if (!args[globalIndexHashKey]) {
        throw new Error(
          `No HashKey for global index. HashKey: ${globalIndexHashKey}`
        );
      }
      hashV.value = args[globalIndexHashKey];
      delete args[globalIndexHashKey];
    } else {
      hashV.value = args[hashKey];
      delete args[hashKey];
    }
  }

  let hashValue = hashV.value;

  if (args) {
    (args => {
      Object.keys(args).forEach(argKey => {
        if (argKey === "dummy") return;
        if (args[argKey] === null) return;
        if (argKey === hashKey) return;
        const retType = getSchemaType(tableSchema, argKey);

        // console.log("retType: ", retType);
        const argType = getType(JSON.parse(JSON.stringify(args[argKey])));
        // console.log("retType", retType, "argType", argType);

        if (retType === "String") {
          if (argType === "List") {
            conditions[argKey] = { $in: args[argKey] };
          } else {
            if (match === "contains") {
              conditions[argKey] = {
                $regex: `${args[argKey]}`,
                $options: "i"
              };
            } else {
              conditions[argKey] = args[argKey];
            }
          }
        }

        if (retType === "Number") {
          conditions[argKey] = args[argKey];
        }

        if (retType === "Boolean") {
          conditions[argKey] = args[argKey];
        }

        if (retType === "Date") {
          conditions[argKey] = {};
          if (args[argKey].begin) {
            conditions[argKey][`$gte`] = new Date(args[argKey].begin);
          }
          if (args[argKey].end) {
            conditions[argKey][`$lte`] = new Date(args[argKey].end);
          }
        }

        if (retType === "Map") {
          const flattenObject = flatten({
            [argKey]: args[argKey]
          });

          Object.keys(flattenObject).forEach(flattenKey => {
            const value = flattenObject[flattenKey];
            if (typeof value === "string" && match === "contains") {
              conditions[flattenKey] = { $regex: `${value}`, $options: "i" };
            } else {
              conditions[flattenKey] = value;
            }
          });
        }

        if (retType === "List" && ["String", "Number"].includes(argType)) {
          if (args[argKey][0] === "!") {
            conditions[argKey] = { $ne: args[argKey].substring(1) };
          } else {
            conditions[argKey] = args[argKey];
          }
        }
      });
    })(JSON.parse(JSON.stringify(args)));
  }

  // remove keys which has no value due to mongodb bugs
  // Object.keys(conditions).forEach(key => {
  //   if (!conditions[key]) {
  //     if (conditions[key] === 0) {
  //       return;
  //     }
  //     delete conditions[key];
  //   }
  // });

  const applyOptions = (conditions, hashValue, comparison) => {
    const map2arr = args => {
      return Object.keys(args).reduce((acc, cur) => {
        const tmp = {};
        tmp[cur] = args[cur];
        acc.push(tmp);
        return acc;
      }, []);
    };

    let newConditions = {};

    if (comparison !== "or") {
      newConditions = Object.assign(conditions);
    } else {
      const or = map2arr(conditions);
      if (or.length) newConditions = { $or: or };
    }

    if (hashValue) newConditions[hashKey] = hashValue;

    return newConditions;
  };

  const finalFilter = {
    conditions: applyOptions(conditions, hashValue, comparison)
    // page: {
    //   limit: 100,
    //   sort: 1
    // }
  };

  // console.log(
  //   JSON.stringify(finalFilter.conditions),
  //   `match: ${match} comparison: ${comparison}`
  // );

  return finalFilter;
};

const reDefineForMongo = schema => {
  const reformat = schema => {
    const newValue = {};

    for (var key in schema) {
      if (typeof schema[key] === "object") {
        if (schema[key].hasOwnProperty("type")) {
          if (schema[key].type === "list") {
            const tmp = schema[key].list;
            delete schema[key].type;
            delete schema[key].list;
            newValue[key] = [reformat(tmp[0])];
            continue;
          }
        }

        if (Array.isArray(schema[key])) {
          newValue[key] = [reformat(schema[key][0])];
        } else {
          newValue[key] = reformat(schema[key]);
        }
      } else {
        newValue[key] = schema[key];
      }
    }

    return newValue;
  };

  const removeIndex = obj => {
    const ret = Object.keys(obj).reduce((acc, cur) => {
      if (obj[cur].index) {
        acc[cur] = Object.assign(obj[cur]);
        delete acc[cur].index;
        return acc;
      }
      acc[cur] = obj[cur];
      return acc;
    }, {});
    return ret;
  };

  return removeIndex(reformat(schema));
};

const createIndex = (schema, hashKey, rangeKey, indexs) => {
  const indexTmp = [];

  if (rangeKey) {
    indexTmp.push({
      key: { [hashKey]: 1, [rangeKey]: 1 },
      name: rangeKey,
      unique: true
    });
  } else {
    indexTmp.push({
      key: { [hashKey]: 1 },
      name: hashKey,
      unique: true
    });
  }

  if (indexs.length) {
    indexs.forEach(el =>
      indexTmp.push({ key: { [hashKey]: 1, [el]: 1 }, name: el })
    );
  }

  indexTmp.forEach(el => {
    schema.index(el.key, { unique: !!el.unique, name: el.name });
  });
  return indexs;
};

const createCollection = (collectionName, model) => {
  const newTable = mongoose.model(collectionName, model, collectionName);
  return newTable;
};

const api = (defs, tableName, prefix) => {
  const obj = {};

  const schema = new mongoose.Schema(reDefineForMongo(defs), {
    strictQuery: true,
    toObject: { getters: true, setters: true }
    // _id: false
  });
  schema.plugin(mongoosePaginate);

  const [hashKey, rangeKey, indexs] = getHashRangeKeyIndex(defs);

  createIndex(schema, hashKey, rangeKey, indexs);

  const table = process.env.MONGODB_PLURAL_COLLECTION_NAME
    ? mongoose.model(tableName, schema)
    : mongoose.model(
        tableName,
        schema,
        prefix ? `${prefix}${tableName}` : tableName
      );
  obj.tableName = `${prefix}${tableName}`;
  obj.tableSchema = schema;
  obj.getRequiredKeys = sortKey => {
    return getRequiredKeys(schema.obj, sortKey);
  };

  obj.insert = async (args, context) => {
    let newArgs = {};
    if (context && context.pre) {
      newArgs = await context.pre(args, context, false);
    } else {
      newArgs = args;
    }
    return table
      .create(newArgs)
      .then(async result => {
        if (context && context.post) {
          await context.post(
            tableName,
            CREATE,
            newArgs,
            rangeKey,
            result,
            context
          );
        }
        return result;
      })
      .catch(e => {
        if (context && context.error) context.error(e);
        else throw new Error(e);
      });
  };

  obj.batchPut = async (args, context, isUpdate = false) => {
    const newArgs = await Promise.all(
      args.map(async el => {
        if (tableName === "log") {
          return el;
        }
        if (context && context.pre) {
          el = await context.pre(el, context, isUpdate);
        }
        return el;
      })
    );

    return table
      .bulkWrite(
        newArgs.map(item => {
          return {
            updateOne: {
              filter: getUniqueKey(item),
              update: JSON.parse(JSON.stringify(item)),
              upsert: true
            }
          };
        })
      )
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
        return result.ok ? true : false;
      })
      .catch(e => {
        if (context && context.error) context.error(e);
        else throw new Error(e);
      });
  };

  const getUniqueKey = args => {
    const keys = {};
    if (hashKey) keys[hashKey] = args[hashKey];
    if (rangeKey) keys[rangeKey] = args[rangeKey];
    return keys;
  };

  obj.update = async (args, context) => {
    let newArgs = {};
    if (context && context.pre) {
      newArgs = await context.pre(args, context, true);
    } else {
      newArgs = args;
    }
    return table
      .findOneAndUpdate(getUniqueKey(newArgs), newArgs, { new: true })
      .then(async result => {
        if (context && context.post) {
          await context.post(
            tableName,
            UPDATE,
            newArgs,
            rangeKey,
            result,
            context
          );
        }
        return result;
        // return result.id ? true : false;
      })
      .catch(e => {
        if (context && context.error) context.error(e);
        else throw new Error(e);
      });
  };

  obj.remove = async (args, context) => {
    return table
      .findOneAndDelete(getUniqueKey(args))
      .then(async result => {
        if (result) {
          if (context && context.post) {
            await context.post(
              tableName,
              DELETE,
              args,
              rangeKey,
              result,
              context
            );
          }
        } else {
          if (context && context.post) {
            await context.post(
              tableName,
              DELETE,
              {},
              rangeKey,
              result,
              context
            );
          }
        }
        return result;
      })
      .catch(e => {
        if (context && context.error) context.error(e);
        else throw new Error(e);
      });
  };

  obj.get = async args =>
    await table
      .findOne(args)
      .then(data => {
        return data ? data.toObject() : null;
      })
      .catch(e => {
        throw new Error(e);
      });

  obj.batchGet = async args =>
    await Promise.all(
      args.map(async item => {
        return await table.findOne(item).catch(e => {
          throw new Error(e);
        });
      })
    );

  obj.batchRemove = async (args = [], context) => {
    return table
      .bulkWrite(
        args.map(el => {
          return {
            deleteOne: {
              filter: getUniqueKey(el)
            }
          };
        })
      )
      .then(async result => {
        if (context && context.post) {
          for (let i = 0; i < args.length; i++) {
            await context.post(
              tableName,
              DELETE,
              args[i],
              rangeKey,
              args[i],
              context
            );
          }
        }

        return result.ok ? true : false;
      })
      .catch(e => {
        if (context && context.error) context.error(e);
        else throw new Error(e);
      });
  };

  obj.increment = (args, target, value = 1) => {
    return new Promise((resolve, reject) => {
      table
        .findOneAndUpdate(
          args,
          { $inc: { [target]: value } },
          { returnOriginal: false }
        )
        .then(data => {
          resolve(data[target]);
        })
        .catch(e => {
          reject(false);
          throw new Error(e);
        });
    });
  };

  obj.decrement = (args, target, value = 1) => {
    return new Promise((resolve, reject) => {
      table
        .findOneAndUpdate(
          args,
          { $inc: { [target]: -value } },
          { returnOriginal: false }
        )
        .then(data => {
          resolve(data[target]);
        })
        .catch(e => {
          reject(false);
          throw new Error(e);
        });
    });
  };

  obj.incrementMap = (args, target, type, value = 1) => {
    const key = `${target}.${type}`;
    return new Promise((resolve, reject) => {
      table
        .findOneAndUpdate(
          { hospitalId: args.hospitalId },
          { $inc: { [key]: value } },
          { returnOriginal: false }
        )
        .then(data => resolve(data[target][type]))
        .catch(e => {
          reject(false);
          throw new Error(e);
        });
    });
  };

  obj.searchAll = async (args = {}, comparison = "and", match = "eq", sort) => {
    return obj.queryAll(args, comparison, match, sort);
  };

  obj.queryAll = async (
    args = {},
    comparison = "and",
    match = "eq",
    sort,
    customFilter = {}
  ) => {
    const { conditions } = createCommonFilter(
      args,
      null,
      null,
      comparison,
      match,
      obj,
      sort,
      null,
      customFilter
    );
    const options = {};

    if (sort) {
      options["sort"] = `${sort.order ? "-" : ""}${
        sort.indexKey.split("_")[1] === "global"
          ? sort.indexKey.split("_")[0]
          : sort.indexKey
      }`;
      options["_id"] = 0;
    }

    return table
      .find(conditions, "", options)
      .then(data => {
        // convert mongoose Object to JS Object to decouple from mongoose
        return data.map(item => item.toObject());
      })
      .catch(e => {
        throw new Error(e);
      });
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
    return queryPagination(args, startKey, limit, comparison, match, sort);
  };

  obj.queryPagination = async (
    args,
    startKey = null,
    limit = 100,
    comparison = "and",
    match = "eq",
    sort = {
      key: "id",
      order: null
    }
  ) => {
    const { conditions } = createCommonFilter(
      args,
      startKey,
      limit,
      comparison,
      match,
      obj,
      sort
    );
    const options = {
      customLabels: { docs: "items" },
      limit: limit,
      page: startKey ? startKey : 1
    };
    if (sort) {
      options["sort"] = `${sort.order ? "-" : ""}${sort.indexKey}`;
      options["_id"] = 0;
    }

    return table
      .paginate(conditions, options)
      .then(data => {
        data.items.lastKey = data.hasNextPage ? data.nextPage : null;
        return data.items;
      })
      .catch(e => {
        throw new Error(e);
      });
  };

  obj.totalCnt = async (args = {}, comparison = "and", match = "eq", sort) => {
    const { conditions } = createCommonFilter(
      args,
      null,
      null,
      comparison,
      match,
      obj,
      sort,
      null
    );
    const options = {};

    if (sort) {
      options["sort"] = `${sort.order ? "-" : ""}${sort.indexKey}`;
      options["_id"] = 0;
    }

    return table
      .find(conditions, "", options)
      .then(data => {
        return data.length;
      })
      .catch(e => {
        throw new Error(e);
      });
  };

  obj.removeAndcreate = async del => {
    if (del) {
      // 테이블 안의 내용을 모두 지우는 작업
      // table.remove({}).then(data => console.log("테이블 내용 전체 삭제"));

      // 테이블 자체를 지우고 생성하는 작업
      table.collection
        .drop()
        .then(data => {
          // console.log("테이블 삭제");
          createCollection(obj.tableName, schema);
          // console.log("테이블 생성");
        })
        .catch(e => {
          throw new Error(e);
        });
    }
  };

  return obj;
};
export { createModels, api };
