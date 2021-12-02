export { api as mongo, createModels as mongoc } from "./mongoDB/api/mongoDB.js";
export {
  api as dynamo,
  checkStatusToDynamoDbName,
  createModels as dynamoc,
  getSecretString,
  restoreTableFromBackup
} from "./dynamoDB/api/dynamoDB.js";
