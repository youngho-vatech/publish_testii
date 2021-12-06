const {
  dynamoDBApi,
  checkStatusToDynamoDbName,
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup
} = require("./dynamoDB/api/dynamoDB.js");
const { mongoDBApi, mongoDBCreateModels } = require("./mongoDB/api/mongoDB.js");
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
  getGlobalIndexHashKey
} = require("./utils/etc.js");

module.exports = {
  dynamoDBApi,
  checkStatusToDynamoDbName,
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
