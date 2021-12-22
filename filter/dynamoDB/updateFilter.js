const {
  getHashRangeKeyIndex,
  getRangeKey,
  getRequiredKeys,
  getType,
  getHashKey,
  splitForEach,
  getGlobalIndexHashKey
} = require("../../utils/etc.js");

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

module.exports = { createUpdateFilter };
