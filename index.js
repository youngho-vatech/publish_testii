const {
  dynamoDBApi,
  checkStatusToDynamoDbName,
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup
} = require("./database/dynamoDB.js");
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
