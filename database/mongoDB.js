const mongoose = require("mongoose");
const {
  envTestBool,
  getHashKey,
  getRequiredKeys,
  getHashRangeKeyIndex,
  getType,
  getGlobalIndexHashKey,
  reDefineForMongo,
  createIndex
} = require("../utils/etc.js");

const mongoosePaginate = require("mongoose-paginate-v2");
const { Schema } = mongoose;
const { createCommonFilter } = require("../filter/mongoDBFilter");
const CREATE = "0";
const UPDATE = "1";
const DELETE = "2";

const mongoDBCreateModels = async options => {
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
      useUnifiedTopology: envTestBool(process.env.MONGODB_UNIFIEDTOPOLOGY),
      socketTimeoutMS: 90000
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
      acc[cur.toLowerCase()] = mongoDBApi(schema[cur], cur, prefix || "");
      return acc;
    }, {});
  }

  return models;
};

const createCollection = (collectionName, model) => {
  const newTable = mongoose.model(collectionName, model, collectionName);
  return newTable;
};

const mongoDBApi = (defs, tableName, prefix) => {
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
        sort.indexKey
          ? sort.indexKey.split("_")[1] === "global"
            ? sort.indexKey.split("_")[0]
            : sort.indexKey
          : ""
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
      options["sort"] = `${sort.order ? "-" : ""}${
        sort.indexKey
          ? sort.indexKey.split("_")[1] === "global"
            ? sort.indexKey.split("_")[0]
            : sort.indexKey
          : ""
      }`;
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
      // ????????? ?????? ????????? ?????? ????????? ??????
      // table.remove({}).then(data => console.log("????????? ?????? ?????? ??????"));

      // ????????? ????????? ????????? ???????????? ??????
      table.collection
        .drop()
        .then(data => {
          // console.log("????????? ??????");
          createCollection(obj.tableName, schema);
          // console.log("????????? ??????");
        })
        .catch(e => {
          throw new Error(e);
        });
    }
  };

  return obj;
};

module.exports = { mongoDBCreateModels, mongoDBApi };
