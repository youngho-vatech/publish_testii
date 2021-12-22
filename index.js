const { dynamoDBApi, dynamoDBCreateModels } = require("./database/dynamoDB.js");
const { mongoDBApi, mongoDBCreateModels } = require("./database/mongoDB.js");
const {
  envTestBool,
  getDateToTimestamp,
  getHashRangeKeyIndex,
  getHashKey,
  getRangeKey,
  getRequiredKeys,
  getType,
  getUniqueKey,
  splitForEach,
  getGlobalIndexHashKey,
  getSecretString,
  restoreTableFromBackup
} = require("./utils/etc.js");

module.exports = {
  dynamoDBApi,
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup,
  mongoDBApi,
  mongoDBCreateModels,
  envTestBool,
  getDateToTimestamp,
  getHashRangeKeyIndex,
  getHashKey,
  getRangeKey,
  getRequiredKeys,
  getType,
  getUniqueKey,
  splitForEach,
  getGlobalIndexHashKey
};
