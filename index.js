const {
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup,
  dynamoDBApi
} = require("./database/dynamoDB.js");
const { mongoDBApi, mongoDBCreateModels } = require("./database/mongoDB.js");
const {
  getType,
  getRequiredKeys,
  getHashRangeKeyIndex,
  getHashKey,
  getRangeKey,
  envTestBool,
  getDateToTimestamp,
  getUniqueKey,
  splitForEach,
  getGlobalIndexHashKey,
  reDefineForMongo,
  createIndex
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
