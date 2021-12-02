const {
  api,
  checkStatusToDynamoDbName,
  createModels,
  getSecretString,
  restoreTableFromBackup
} = require("./dynamoDB/api/index");

function test(params) {
  console.log("asdf", params);
}
module.exports = api;
module.exports = test;
