const { flatten } = require("flat");
const mongoose = require("mongoose");
const { Schema } = mongoose;
const {
  envTestBool,
  getHashKey,
  getRequiredKeys,
  getHashRangeKeyIndex,
  getType,
  getGlobalIndexHashKey
} = require("../utils/etc.js");

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
  if (sort && sort.indexKey) {
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
  };

  return finalFilter;
};

module.exports = { createCommonFilter };
