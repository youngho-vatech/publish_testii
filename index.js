const {
  dynamoDBApi,
  checkStatusToDynamoDbName,
  dynamoDBCreateModels,
  getSecretString,
  restoreTableFromBackup
} = require("./dynamoDB/api/dynamoDB.js");
const { mongoDBApi, mongoDBCreateModels } = require("./mongoDB/api/mongoDB.js");

module.exports = {
  test,
  testtest,
  api,
  checkStatusToDynamoDbName,
  createModels,
  getSecretString,
  restoreTableFromBackup
};
