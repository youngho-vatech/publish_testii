import {
  api as dynamoApi,
  checkStatusToDynamoDbName,
  createModels as dynamoCreateModels,
  getSecretString,
  restoreTableFromBackup
} from "./dynamoDB/api/index.js";
export {
  api as mongoApi,
  createModels as mongoCreateModels
} from "./mongoDB/api/index.js";

function test(params) {
  console.log("asdf", params);
}
module.exports = dynamoApi;
module.exports = test;
